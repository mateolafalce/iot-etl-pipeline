"""
Clean Step
==========
Validates and normalises a DataFrame of raw IoT events pulled from the
``raw_events`` table.

Responsibilities
----------------
1. Remove exact duplicates (same sensor_id + timestamp).
2. Route rows with null ``value`` to dead-letter with an explanatory reason.
3. Route rows with null / unparseable ``timestamp`` to dead-letter.
4. Validate ``value`` ranges per sensor type; out-of-range -> dead-letter.
5. Normalise timestamps to UTC (TIMESTAMPTZ-compatible ISO string).
6. Cast ``value`` column to Float64.
7. Return a clean Polars DataFrame and a dead-letter Polars DataFrame.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timezone
from typing import List, Tuple

import polars as pl
import psycopg2
import psycopg2.extras
from dateutil import parser as dateutil_parser
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Acceptable value ranges per sensor_type
# ---------------------------------------------------------------------------
VALID_RANGES: dict[str, Tuple[float, float]] = {
    "temperature": (-50.0, 100.0),
    "motion": (0.0, 1.0),
    "door": (0.0, 1.0),
}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "iot_db")
PG_USER = os.getenv("POSTGRES_USER", "iot_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iot_pass")


def get_db_connection() -> psycopg2.extensions.connection:
    """Return an open psycopg2 connection."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )


def fetch_unprocessed_raw_events(conn: psycopg2.extensions.connection) -> List[dict]:
    """
    Fetch all unprocessed raw events from the database.

    Returns a list of dicts with flattened payload fields:
    id, sensor_id, sensor_type, value, unit, timestamp, payload (JSON string).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id,
                   sensor_id,
                   sensor_type,
                   payload,
                   received_at::text AS received_at,
                   processed
            FROM raw_events
            WHERE processed = FALSE
            ORDER BY received_at
            LIMIT 5000
            """
        )
        rows = [dict(r) for r in cur.fetchall()]

    flattened: List[dict] = []
    for row in rows:
        payload = row.get("payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        flat = {
            "id": row["id"],
            "sensor_id": row.get("sensor_id") or payload.get("sensor_id"),
            "sensor_type": row.get("sensor_type") or payload.get("sensor_type"),
            "value": payload.get("value"),
            "unit": payload.get("unit"),
            "timestamp": payload.get("timestamp"),
            "received_at": row.get("received_at"),
            "payload": json.dumps(payload),
        }
        flattened.append(flat)

    logger.info("fetch_unprocessed_raw_events: fetched %d rows.", len(flattened))
    return flattened


def validate_and_clean(
    records: List[dict],
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Clean and validate raw sensor event records.

    Parameters
    ----------
    records:
        List of dicts as returned by ``fetch_unprocessed_raw_events``.

    Returns
    -------
    (clean_df, dead_letter_df)
    """
    if not records:
        empty_clean = pl.DataFrame(
            schema={
                "raw_event_id": pl.Int64,
                "sensor_id": pl.Utf8,
                "sensor_type": pl.Utf8,
                "value": pl.Float64,
                "unit": pl.Utf8,
                "event_ts": pl.Utf8,
            }
        )
        empty_dead = pl.DataFrame(
            schema={
                "raw_event_id": pl.Int64,
                "payload": pl.Utf8,
                "reason": pl.Utf8,
            }
        )
        return empty_clean, empty_dead

    raw_df = pl.DataFrame(records)
    return clean_events(raw_df)


def clean_events(
    raw_df: pl.DataFrame,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Clean and validate raw sensor events.

    Parameters
    ----------
    raw_df:
        DataFrame with columns:
        ``id``, ``sensor_id``, ``sensor_type``, ``payload`` (struct/dict),
        ``received_at``, ``processed``.

        The ``payload`` column must be already exploded so that columns
        ``value``, ``unit``, and ``timestamp`` exist at the top level.

    Returns
    -------
    (clean_df, dead_letter_df)
        ``clean_df``       - validated events, ready for enrichment.
        ``dead_letter_df`` - rejected events with a ``reason`` column appended.
    """
    if raw_df.is_empty():
        logger.info("clean_events: input DataFrame is empty, nothing to clean.")
        empty_clean = pl.DataFrame(
            schema={
                "raw_event_id": pl.Int64,
                "sensor_id": pl.Utf8,
                "sensor_type": pl.Utf8,
                "value": pl.Float64,
                "unit": pl.Utf8,
                "event_ts": pl.Utf8,
            }
        )
        empty_dead = pl.DataFrame(
            schema={
                "raw_event_id": pl.Int64,
                "payload": pl.Utf8,
                "reason": pl.Utf8,
            }
        )
        return empty_clean, empty_dead

    logger.info("clean_events: processing %d raw rows.", raw_df.height)

    # Work on a mutable copy; track rejected rows separately
    df = raw_df.clone()
    dead_letter_rows: list[dict] = []

    # ------------------------------------------------------------------
    # Helper: collect rejected rows into dead-letter list
    # ------------------------------------------------------------------
    def _reject(subset: pl.DataFrame, reason: str) -> None:
        for row in subset.to_dicts():
            dead_letter_rows.append(
                {
                    "raw_event_id": row.get("id"),
                    "payload": str(row.get("payload", "")),
                    "reason": reason,
                }
            )

    # ------------------------------------------------------------------
    # Step 1 - Deduplicate on (sensor_id, timestamp)
    # ------------------------------------------------------------------
    before = df.height
    df = df.unique(subset=["sensor_id", "timestamp"], keep="first", maintain_order=True)
    removed = before - df.height
    if removed:
        logger.info("Deduplication removed %d duplicate rows.", removed)

    # ------------------------------------------------------------------
    # Step 2 - Null value filter
    # ------------------------------------------------------------------
    null_value_mask = df["value"].is_null()
    if null_value_mask.sum() > 0:
        _reject(df.filter(null_value_mask), "null value")
        df = df.filter(~null_value_mask)
        logger.info("Removed %d rows with null value.", null_value_mask.sum())

    # ------------------------------------------------------------------
    # Step 3 - Null / unparseable timestamp filter
    # ------------------------------------------------------------------
    null_ts_mask = df["timestamp"].is_null()
    if null_ts_mask.sum() > 0:
        _reject(df.filter(null_ts_mask), "null timestamp")
        df = df.filter(~null_ts_mask)
        logger.info("Removed %d rows with null timestamp.", null_ts_mask.sum())

    # Parse timestamps - flag rows whose timestamp string cannot be parsed
    parsed_ts: list[str | None] = []
    bad_ts_indices: list[int] = []
    for idx, row in enumerate(df.to_dicts()):
        raw_ts = row.get("timestamp")
        try:
            dt = dateutil_parser.parse(str(raw_ts))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            parsed_ts.append(dt.isoformat())
        except (ValueError, OverflowError, TypeError):
            parsed_ts.append(None)
            bad_ts_indices.append(idx)

    if bad_ts_indices:
        bad_ts_df = df[bad_ts_indices]
        _reject(bad_ts_df, "unparseable timestamp")
        good_indices = [i for i in range(df.height) if i not in set(bad_ts_indices)]
        df = df[good_indices]
        parsed_ts = [ts for i, ts in enumerate(parsed_ts) if i not in set(bad_ts_indices)]
        logger.info("Removed %d rows with unparseable timestamps.", len(bad_ts_indices))

    # Replace raw timestamp column with normalised UTC strings
    df = df.with_columns(pl.Series("event_ts", parsed_ts))

    # ------------------------------------------------------------------
    # Step 4 - Cast value to Float64
    # ------------------------------------------------------------------
    df = df.with_columns(pl.col("value").cast(pl.Float64))

    # ------------------------------------------------------------------
    # Step 5 - Range validation per sensor_type
    # ------------------------------------------------------------------
    range_bad_indices: list[int] = []
    rows_list = df.to_dicts()
    for idx, row in enumerate(rows_list):
        s_type = str(row.get("sensor_type", "")).lower()
        val = row.get("value")
        if s_type in VALID_RANGES and val is not None:
            lo, hi = VALID_RANGES[s_type]
            if not (lo <= float(val) <= hi):
                range_bad_indices.append(idx)

    if range_bad_indices:
        bad_range_df = df[range_bad_indices]
        _reject(bad_range_df, "value out of valid range for sensor_type")
        good_indices = [i for i in range(df.height) if i not in set(range_bad_indices)]
        df = df[good_indices]
        logger.info("Removed %d rows with out-of-range values.", len(range_bad_indices))

    # ------------------------------------------------------------------
    # Step 6 - Build final clean DataFrame
    # ------------------------------------------------------------------
    # Ensure unit column exists (may be absent from some payloads)
    if "unit" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("unit"))

    if df.is_empty():
        clean_df = pl.DataFrame(
            schema={
                "raw_event_id": pl.Int64,
                "sensor_id": pl.Utf8,
                "sensor_type": pl.Utf8,
                "value": pl.Float64,
                "unit": pl.Utf8,
                "event_ts": pl.Utf8,
            }
        )
    else:
        clean_df = df.select(
            [
                pl.col("id").alias("raw_event_id"),
                pl.col("sensor_id").cast(pl.Utf8),
                pl.col("sensor_type").cast(pl.Utf8),
                pl.col("value").cast(pl.Float64),
                pl.col("unit").cast(pl.Utf8),
                pl.col("event_ts").cast(pl.Utf8),
            ]
        )

    # ------------------------------------------------------------------
    # Step 7 - Build dead-letter DataFrame
    # ------------------------------------------------------------------
    dead_letter_df = (
        pl.DataFrame(dead_letter_rows)
        if dead_letter_rows
        else pl.DataFrame(
            schema={
                "raw_event_id": pl.Int64,
                "payload": pl.Utf8,
                "reason": pl.Utf8,
            }
        )
    )

    logger.info(
        "clean_events complete: %d clean rows, %d dead-letter rows.",
        clean_df.height,
        dead_letter_df.height,
    )
    return clean_df, dead_letter_df


def write_clean_events(
    conn: psycopg2.extensions.connection, clean_df: pl.DataFrame
) -> List[int]:
    """
    Batch insert clean events into the ``clean_events`` table.

    Returns
    -------
    List of inserted row IDs.
    """
    if clean_df.is_empty():
        return []

    rows = clean_df.to_dicts()
    inserted_ids: List[int] = []

    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO clean_events
                    (raw_event_id, sensor_id, sensor_type, value, unit, event_ts)
                VALUES
                    (%(raw_event_id)s, %(sensor_id)s, %(sensor_type)s,
                     %(value)s, %(unit)s, %(event_ts)s)
                RETURNING id
                """,
                {
                    "raw_event_id": row.get("raw_event_id"),
                    "sensor_id": row.get("sensor_id"),
                    "sensor_type": row.get("sensor_type"),
                    "value": row.get("value"),
                    "unit": row.get("unit"),
                    "event_ts": row.get("event_ts"),
                },
            )
            result = cur.fetchone()
            if result:
                inserted_ids.append(result[0])

    conn.commit()
    logger.info("write_clean_events: inserted %d rows into clean_events.", len(inserted_ids))
    return inserted_ids


def write_dead_letters(
    conn: psycopg2.extensions.connection, dead_df: pl.DataFrame
) -> None:
    """Batch insert dead-letter events into the ``dead_letter_events`` table."""
    if dead_df.is_empty():
        return

    rows = dead_df.to_dicts()
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO dead_letter_events (raw_event_id, reason, payload)
            VALUES (%(raw_event_id)s, %(reason)s, %(payload)s)
            """,
            [
                {
                    "raw_event_id": r.get("raw_event_id"),
                    "reason": r.get("reason"),
                    "payload": psycopg2.extras.Json({"raw": r.get("payload", "")}),
                }
                for r in rows
            ],
            page_size=500,
        )
    conn.commit()
    logger.info("write_dead_letters: inserted %d dead-letter rows.", len(rows))


def run_clean(**kwargs) -> None:
    """
    Airflow task callable: orchestrate the clean step end-to-end.

    1. Fetch unprocessed raw events from ``raw_events``.
    2. Validate and clean them.
    3. Write clean events to ``clean_events``.
    4. Write rejected events to ``dead_letter_events``.
    5. Push clean event IDs via XCom for the enrich step.
    """
    ti = kwargs.get("ti")

    conn = get_db_connection()
    try:
        records = fetch_unprocessed_raw_events(conn)

        if not records:
            logger.info("run_clean: no unprocessed events found.")
            if ti:
                ti.xcom_push(key="clean_event_ids", value=[])
                ti.xcom_push(key="raw_event_ids", value=[])
                ti.xcom_push(key="clean_records_json", value="[]")
            return

        raw_event_ids = [r["id"] for r in records if r.get("id") is not None]

        clean_df, dead_df = validate_and_clean(records)

        clean_ids = write_clean_events(conn, clean_df)
        write_dead_letters(conn, dead_df)

        logger.info(
            "run_clean complete: %d clean, %d dead-letter.",
            len(clean_ids),
            dead_df.height,
        )

        if ti:
            ti.xcom_push(key="clean_event_ids", value=clean_ids)
            ti.xcom_push(key="raw_event_ids", value=raw_event_ids)
            # Also push the clean DataFrame as JSON for downstream tasks
            ti.xcom_push(key="clean_records_json", value=clean_df.write_json())

    except Exception:
        logger.exception("run_clean: unexpected error.")
        raise
    finally:
        conn.close()
