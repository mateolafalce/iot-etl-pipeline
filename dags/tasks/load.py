"""
Load Step
=========
Persists enriched sensor readings and detected anomalies to their respective
PostgreSQL tables using ``psycopg2`` batch inserts.

Tables written
--------------
- ``sensor_readings``    - final analytical / reporting table.
- ``anomalies``          - detected anomalous events.
- ``dead_letter_events`` - events rejected during the clean step.
"""

from __future__ import annotations

import logging
import os
from typing import List, Optional

import polars as pl
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database connection helpers
# ---------------------------------------------------------------------------

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "iot_db")
PG_USER = os.getenv("POSTGRES_USER", "iot_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iot_pass")


def _get_connection() -> psycopg2.extensions.connection:
    """Return an open psycopg2 connection."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )


# ---------------------------------------------------------------------------
# SQL statements
# ---------------------------------------------------------------------------

_INSERT_SENSOR_READINGS = """
    INSERT INTO sensor_readings
        (sensor_id, sensor_type, value, unit, event_ts,
         hour_of_day, day_of_week, zone, location,
         is_business_hours, ingested_at)
    VALUES
        (%(sensor_id)s, %(sensor_type)s, %(value)s, %(unit)s,
         %(event_ts)s, %(hour_of_day)s, %(day_of_week)s,
         %(zone)s, %(location)s, %(is_business_hours)s, NOW())
"""

_INSERT_ANOMALIES = """
    INSERT INTO anomalies
        (sensor_id, sensor_type, event_ts, value, anomaly_type, score, reason)
    VALUES
        (%(sensor_id)s, %(sensor_type)s, %(event_ts)s, %(value)s,
         %(anomaly_type)s, %(score)s, %(reason)s)
"""

_MARK_PROCESSED = """
    UPDATE raw_events
    SET processed = TRUE
    WHERE id = ANY(%(ids)s)
"""


# ---------------------------------------------------------------------------
# Row coercion helpers
# ---------------------------------------------------------------------------

def _coerce_row(row: dict) -> dict:
    """Convert polars nulls / special types to Python-native values."""
    return {k: (None if v != v else v) for k, v in row.items()}  # NaN -> None


def _df_to_dicts(df: pl.DataFrame) -> List[dict]:
    return [_coerce_row(r) for r in df.to_dicts()]


# ---------------------------------------------------------------------------
# Public API - individual load functions
# ---------------------------------------------------------------------------

def load_sensor_readings(
    conn: psycopg2.extensions.connection,
    enriched_records: List[dict],
) -> None:
    """
    Batch INSERT enriched sensor events into ``sensor_readings``.

    Parameters
    ----------
    conn:
        Open psycopg2 connection.
    enriched_records:
        List of dicts representing enriched sensor events.
    """
    if not enriched_records:
        logger.info("load_sensor_readings: nothing to insert.")
        return

    required_cols = (
        "sensor_id", "sensor_type", "value", "unit", "event_ts",
        "hour_of_day", "day_of_week", "zone", "location", "is_business_hours",
    )
    rows = [
        {col: r.get(col) for col in required_cols}
        for r in enriched_records
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _INSERT_SENSOR_READINGS, rows, page_size=500)
    conn.commit()
    logger.info("load_sensor_readings: inserted %d rows into sensor_readings.", len(rows))


def load_anomalies(
    conn: psycopg2.extensions.connection,
    anomaly_records: List[dict],
) -> None:
    """
    Batch INSERT anomaly events into the ``anomalies`` table.

    Parameters
    ----------
    conn:
        Open psycopg2 connection.
    anomaly_records:
        List of dicts representing detected anomalies.
    """
    if not anomaly_records:
        logger.info("load_anomalies: nothing to insert.")
        return

    required_cols = (
        "sensor_id", "sensor_type", "event_ts", "value",
        "anomaly_type", "score", "reason",
    )
    rows = [
        {col: r.get(col) for col in required_cols}
        for r in anomaly_records
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _INSERT_ANOMALIES, rows, page_size=500)
    conn.commit()
    logger.info("load_anomalies: inserted %d rows into anomalies.", len(rows))


def mark_raw_events_processed(
    conn: psycopg2.extensions.connection,
    raw_ids: List[int],
) -> None:
    """
    Mark raw_events rows as processed=TRUE.

    Parameters
    ----------
    conn:
        Open psycopg2 connection.
    raw_ids:
        List of ``raw_events.id`` values to mark.
    """
    if not raw_ids:
        logger.info("mark_raw_events_processed: no IDs to update.")
        return

    with conn.cursor() as cur:
        cur.execute(_MARK_PROCESSED, {"ids": raw_ids})
    conn.commit()
    logger.info(
        "mark_raw_events_processed: marked %d raw_events rows as processed.", len(raw_ids)
    )


def load_to_db(
    enriched_df: pl.DataFrame,
    anomalies_df: pl.DataFrame,
    dead_letter_df: Optional[pl.DataFrame] = None,
    raw_event_ids: Optional[List[int]] = None,
) -> None:
    """
    Batch-insert enriched events and anomalies into PostgreSQL.

    Parameters
    ----------
    enriched_df:
        Output of :func:`~dags.tasks.enrich.enrich_events`.
    anomalies_df:
        Output of :func:`~dags.tasks.anomaly.detect_anomalies`.
    dead_letter_df:
        Optional dead-letter events from the clean step.
    raw_event_ids:
        List of ``raw_events.id`` values to mark as processed after a
        successful load.  When *None* nothing is marked.
    """
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # ---- sensor_readings -----------------------------------
                if not enriched_df.is_empty():
                    # Ensure required columns exist with sensible defaults
                    for col in ("zone", "location", "unit"):
                        if col not in enriched_df.columns:
                            enriched_df = enriched_df.with_columns(
                                pl.lit(None).cast(pl.Utf8).alias(col)
                            )

                    readings_rows = _df_to_dicts(
                        enriched_df.select(
                            [
                                "sensor_id",
                                "sensor_type",
                                "value",
                                "unit",
                                "event_ts",
                                "hour_of_day",
                                "day_of_week",
                                "zone",
                                "location",
                                "is_business_hours",
                            ]
                        )
                    )
                    psycopg2.extras.execute_batch(
                        cur, _INSERT_SENSOR_READINGS, readings_rows, page_size=500
                    )
                    logger.info(
                        "Inserted %d rows into sensor_readings.", len(readings_rows)
                    )

                # ---- anomalies -----------------------------------------
                if not anomalies_df.is_empty():
                    anomaly_rows = _df_to_dicts(
                        anomalies_df.select(
                            [
                                "sensor_id",
                                "sensor_type",
                                "event_ts",
                                "value",
                                "anomaly_type",
                                "score",
                                "reason",
                            ]
                        )
                    )
                    psycopg2.extras.execute_batch(
                        cur, _INSERT_ANOMALIES, anomaly_rows, page_size=500
                    )
                    logger.info(
                        "Inserted %d rows into anomalies.", len(anomaly_rows)
                    )

                # ---- dead_letter_events --------------------------------
                if dead_letter_df is not None and not dead_letter_df.is_empty():
                    dl_rows = _df_to_dicts(dead_letter_df)
                    # payload column should be a string; wrap in Json for psycopg2
                    for row in dl_rows:
                        if isinstance(row.get("payload"), str):
                            row["payload"] = psycopg2.extras.Json(
                                {"raw": row["payload"]}
                            )
                    psycopg2.extras.execute_batch(
                        cur,
                        """
                        INSERT INTO dead_letter_events (raw_event_id, reason, payload)
                        VALUES (%(raw_event_id)s, %(reason)s, %(payload)s)
                        """,
                        dl_rows,
                        page_size=500,
                    )
                    logger.info(
                        "Inserted %d rows into dead_letter_events.", len(dl_rows)
                    )

                # ---- mark raw_events as processed ----------------------
                if raw_event_ids:
                    cur.execute(_MARK_PROCESSED, {"ids": raw_event_ids})
                    logger.info(
                        "Marked %d raw_events rows as processed.",
                        len(raw_event_ids),
                    )

        logger.info("load_to_db: all inserts committed successfully.")

    except psycopg2.Error as exc:
        logger.error("load_to_db: database error - %s", exc)
        raise
    finally:
        conn.close()


def run_load(**kwargs) -> None:
    """
    Airflow task callable: load enriched events and anomalies to the database.

    1. Pull enriched_records_json and anomaly_records_json from XCom.
    2. Pull raw_event_ids from XCom (produced by run_clean).
    3. Load enriched events into ``sensor_readings``.
    4. Load anomalies into ``anomalies``.
    5. Mark raw events as processed.
    """
    ti = kwargs.get("ti")

    enriched_json = None
    anomaly_json = None
    raw_event_ids: List[int] = []

    if ti:
        enriched_json = ti.xcom_pull(task_ids="enrich", key="enriched_records_json")
        anomaly_json = ti.xcom_pull(task_ids="detect_anomalies", key="anomaly_records_json")
        raw_event_ids = ti.xcom_pull(task_ids="clean", key="raw_event_ids") or []

    enriched_records: List[dict] = []
    anomaly_records: List[dict] = []

    if enriched_json and enriched_json != "[]":
        try:
            import json as _json
            enriched_records = _json.loads(enriched_json)
        except Exception:
            enriched_df = pl.read_json(enriched_json.encode())
            enriched_records = enriched_df.to_dicts()

    if anomaly_json and anomaly_json != "[]":
        try:
            import json as _json
            anomaly_records = _json.loads(anomaly_json)
        except Exception:
            anomaly_df = pl.read_json(anomaly_json.encode())
            anomaly_records = anomaly_df.to_dicts()

    conn = _get_connection()
    try:
        load_sensor_readings(conn, enriched_records)
        load_anomalies(conn, anomaly_records)
        mark_raw_events_processed(conn, raw_event_ids)
    except psycopg2.Error as exc:
        logger.error("run_load: database error - %s", exc)
        raise
    finally:
        conn.close()

    logger.info(
        "run_load complete: %d readings, %d anomalies, %d raw events marked.",
        len(enriched_records),
        len(anomaly_records),
        len(raw_event_ids),
    )
