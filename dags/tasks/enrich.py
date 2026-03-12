"""
Enrich Step
===========
Joins clean sensor events with ``sensor_metadata`` and adds time-based
feature columns used by the anomaly detection and reporting steps.

Added columns
-------------
- ``hour_of_day``      - integer 0-23
- ``day_of_week``      - integer 0 (Monday) ... 6 (Sunday)
- ``is_business_hours``- True when hour_of_day in [8, 18) AND day_of_week in [0, 4]
- ``zone``             - from sensor_metadata
- ``location``         - from sensor_metadata
- ``min_expected``     - from sensor_metadata
- ``max_expected``     - from sensor_metadata
- ``baseline_events_per_min`` - from sensor_metadata
"""

from __future__ import annotations

import logging
import os
from typing import List

import polars as pl
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "iot_db")
PG_USER = os.getenv("POSTGRES_USER", "iot_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iot_pass")


def _get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )


def fetch_clean_events(
    conn: psycopg2.extensions.connection, ids: List[int]
) -> pl.DataFrame:
    """
    Fetch clean events by ID from the ``clean_events`` table.

    Parameters
    ----------
    conn:
        Open psycopg2 connection.
    ids:
        List of ``clean_events.id`` values to retrieve.

    Returns
    -------
    pl.DataFrame with columns matching the clean_events schema.
    """
    if not ids:
        return pl.DataFrame(
            schema={
                "id": pl.Int64,
                "raw_event_id": pl.Int64,
                "sensor_id": pl.Utf8,
                "sensor_type": pl.Utf8,
                "value": pl.Float64,
                "unit": pl.Utf8,
                "event_ts": pl.Utf8,
            }
        )

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id,
                   raw_event_id,
                   sensor_id,
                   sensor_type,
                   value,
                   unit,
                   event_ts::text AS event_ts,
                   ingested_at::text AS ingested_at
            FROM clean_events
            WHERE id = ANY(%(ids)s)
            ORDER BY event_ts
            """,
            {"ids": ids},
        )
        rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        return pl.DataFrame(
            schema={
                "id": pl.Int64,
                "raw_event_id": pl.Int64,
                "sensor_id": pl.Utf8,
                "sensor_type": pl.Utf8,
                "value": pl.Float64,
                "unit": pl.Utf8,
                "event_ts": pl.Utf8,
            }
        )

    df = pl.DataFrame(rows)
    logger.info("fetch_clean_events: retrieved %d rows.", df.height)
    return df


def fetch_sensor_metadata(conn: psycopg2.extensions.connection) -> pl.DataFrame:
    """
    Fetch all rows from the ``sensor_metadata`` table.

    Returns
    -------
    pl.DataFrame with metadata columns.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM sensor_metadata")
        rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        return pl.DataFrame(
            schema={
                "sensor_id": pl.Utf8,
                "sensor_type": pl.Utf8,
                "location": pl.Utf8,
                "zone": pl.Utf8,
                "min_expected": pl.Float64,
                "max_expected": pl.Float64,
                "baseline_events_per_min": pl.Float64,
            }
        )

    df = pl.DataFrame(rows)
    logger.info("fetch_sensor_metadata: retrieved %d sensor metadata rows.", df.height)
    return df


def enrich_events(
    clean_df: pl.DataFrame,
    metadata_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Enrich a clean-events DataFrame with metadata and temporal features.

    Parameters
    ----------
    clean_df:
        Output of :func:`~dags.tasks.clean.clean_events`.
        Required columns: ``sensor_id``, ``sensor_type``, ``value``, ``unit``,
        ``event_ts`` (ISO-8601 string in UTC).

    metadata_df:
        DataFrame loaded from the ``sensor_metadata`` table.
        Required columns: ``sensor_id``, ``location``, ``zone``,
        ``min_expected``, ``max_expected``, ``baseline_events_per_min``.

    Returns
    -------
    pl.DataFrame
        Enriched DataFrame with all original columns plus the additions
        described in the module docstring.
    """
    if clean_df.is_empty():
        logger.info("enrich_events: input DataFrame is empty, nothing to enrich.")
        return clean_df

    logger.info("enrich_events: enriching %d rows.", clean_df.height)

    # ------------------------------------------------------------------
    # 1. Parse event_ts string -> Datetime (UTC)
    # ------------------------------------------------------------------
    df = clean_df.with_columns(
        pl.col("event_ts")
        .str.strptime(pl.Datetime("us", "UTC"), format=None, strict=False)
        .alias("event_ts_dt")
    )

    # ------------------------------------------------------------------
    # 2. Derive temporal feature columns
    # ------------------------------------------------------------------
    df = df.with_columns(
        [
            pl.col("event_ts_dt").dt.hour().cast(pl.Int32).alias("hour_of_day"),
            # Polars: weekday() returns 1=Monday ... 7=Sunday; we want 0=Monday
            (pl.col("event_ts_dt").dt.weekday() - 1).cast(pl.Int32).alias("day_of_week"),
        ]
    )

    df = df.with_columns(
        (
            (pl.col("hour_of_day") >= 8)
            & (pl.col("hour_of_day") < 18)
            & (pl.col("day_of_week") <= 4)
        ).alias("is_business_hours")
    )

    # ------------------------------------------------------------------
    # 3. Join with sensor metadata
    # ------------------------------------------------------------------
    if not metadata_df.is_empty():
        meta = metadata_df.select(
            [
                pl.col("sensor_id"),
                pl.col("location"),
                pl.col("zone"),
                pl.col("min_expected"),
                pl.col("max_expected"),
                pl.col("baseline_events_per_min"),
            ]
        )
        df = df.join(meta, on="sensor_id", how="left")
    else:
        df = df.with_columns(
            [
                pl.lit(None).cast(pl.Utf8).alias("location"),
                pl.lit(None).cast(pl.Utf8).alias("zone"),
                pl.lit(None).cast(pl.Float64).alias("min_expected"),
                pl.lit(None).cast(pl.Float64).alias("max_expected"),
                pl.lit(None).cast(pl.Float64).alias("baseline_events_per_min"),
            ]
        )

    # ------------------------------------------------------------------
    # 4. Drop the intermediate datetime column; keep string event_ts
    # ------------------------------------------------------------------
    df = df.drop("event_ts_dt")

    logger.info(
        "enrich_events complete: %d enriched rows, %d without metadata.",
        df.height,
        df["zone"].is_null().sum(),
    )
    return df


def run_enrich(**kwargs) -> None:
    """
    Airflow task callable: orchestrate the enrich step end-to-end.

    1. Pull clean_event_ids from XCom (produced by run_clean).
    2. Fetch those events from the DB.
    3. Fetch sensor metadata.
    4. Enrich events.
    5. Push enriched records via XCom for anomaly detection and load steps.
    """
    ti = kwargs.get("ti")

    clean_ids = []
    if ti:
        clean_ids = ti.xcom_pull(task_ids="clean", key="clean_event_ids") or []

    if not clean_ids:
        logger.info("run_enrich: no clean event IDs received, nothing to enrich.")
        if ti:
            ti.xcom_push(key="enriched_records_json", value="[]")
        return

    conn = _get_connection()
    try:
        clean_df = fetch_clean_events(conn, clean_ids)
        metadata_df = fetch_sensor_metadata(conn)
    finally:
        conn.close()

    enriched_df = enrich_events(clean_df, metadata_df)

    logger.info("run_enrich complete: %d enriched rows.", enriched_df.height)

    if ti:
        ti.xcom_push(key="enriched_records_json", value=enriched_df.write_json())
