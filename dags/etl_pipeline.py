"""
IoT ETL Pipeline – Airflow DAG
==============================
Orchestrates the full ETL workflow:

  extract → clean → enrich → detect_anomalies → load → mark_processed

Schedule
--------
Every 5 minutes (``*/5 * * * *``).

Data flow
---------
Each PythonOperator serialises its output as a JSON string pushed to XCom.
Downstream tasks pull the XCom value, deserialise it, and continue.

Because large DataFrames over XCom can be slow, each step passes only the
data it produced (not the full history).  For a production system consider
using a shared volume / object store instead.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database helpers (shared across callables)
# ---------------------------------------------------------------------------

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "iot_db")
PG_USER = os.getenv("POSTGRES_USER", "iot_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iot_pass")


def _pg_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def extract(**context: Any) -> None:
    """
    Pull unprocessed rows from ``raw_events`` and push them as JSON to XCom.

    Also loads ``sensor_metadata`` once and pushes it as a separate XCom key
    so that the enrich step does not need a second DB round-trip.
    """
    conn = _pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Fetch unprocessed raw events
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
            raw_rows = [dict(r) for r in cur.fetchall()]

            # Flatten payload JSONB into top-level columns
            flattened: list[dict] = []
            for row in raw_rows:
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

            # Fetch metadata
            cur.execute("SELECT * FROM sensor_metadata")
            metadata_rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    logger.info("extract: fetched %d unprocessed raw events.", len(flattened))

    ti = context["ti"]
    ti.xcom_push(key="raw_events_json", value=json.dumps(flattened))
    ti.xcom_push(key="metadata_json", value=json.dumps(metadata_rows))


def clean(**context: Any) -> None:
    """Clean raw events and push results to XCom."""
    from dags.tasks.clean import clean_events

    ti = context["ti"]
    raw_json = ti.xcom_pull(task_ids="extract", key="raw_events_json")

    if not raw_json:
        logger.info("clean: no raw events received from extract.")
        ti.xcom_push(key="clean_json", value=json.dumps([]))
        ti.xcom_push(key="dead_letter_json", value=json.dumps([]))
        return

    raw_rows = json.loads(raw_json)
    if not raw_rows:
        ti.xcom_push(key="clean_json", value=json.dumps([]))
        ti.xcom_push(key="dead_letter_json", value=json.dumps([]))
        return

    raw_df = pl.DataFrame(raw_rows)
    clean_df, dead_letter_df = clean_events(raw_df)

    ti.xcom_push(key="clean_json", value=clean_df.write_json())
    ti.xcom_push(key="dead_letter_json", value=dead_letter_df.write_json())
    logger.info(
        "clean: %d clean rows, %d dead-letter rows.", clean_df.height, dead_letter_df.height
    )


def enrich(**context: Any) -> None:
    """Enrich clean events and push results to XCom."""
    from dags.tasks.enrich import enrich_events

    ti = context["ti"]
    clean_json = ti.xcom_pull(task_ids="clean", key="clean_json")
    metadata_json = ti.xcom_pull(task_ids="extract", key="metadata_json")

    if not clean_json or clean_json == "[]":
        logger.info("enrich: no clean events to enrich.")
        ti.xcom_push(key="enriched_json", value=json.dumps([]))
        return

    clean_df = pl.read_json(clean_json.encode())
    metadata_rows = json.loads(metadata_json) if metadata_json else []
    metadata_df = pl.DataFrame(metadata_rows) if metadata_rows else pl.DataFrame()

    enriched_df = enrich_events(clean_df, metadata_df)
    ti.xcom_push(key="enriched_json", value=enriched_df.write_json())
    logger.info("enrich: %d enriched rows.", enriched_df.height)


def detect_anomalies(**context: Any) -> None:
    """Run anomaly detection and push results to XCom."""
    from dags.tasks.anomaly import detect_anomalies as _detect

    ti = context["ti"]
    enriched_json = ti.xcom_pull(task_ids="enrich", key="enriched_json")

    if not enriched_json or enriched_json == "[]":
        logger.info("detect_anomalies: no enriched data.")
        ti.xcom_push(key="anomalies_json", value=json.dumps([]))
        return

    enriched_df = pl.read_json(enriched_json.encode())
    anomalies_df = _detect(enriched_df)

    ti.xcom_push(key="anomalies_json", value=anomalies_df.write_json())
    logger.info("detect_anomalies: %d anomalies detected.", anomalies_df.height)


def load(**context: Any) -> None:
    """Load enriched events and anomalies to the database."""
    from dags.tasks.load import load_to_db

    ti = context["ti"]
    enriched_json = ti.xcom_pull(task_ids="enrich", key="enriched_json")
    anomalies_json = ti.xcom_pull(task_ids="detect_anomalies", key="anomalies_json")
    dead_letter_json = ti.xcom_pull(task_ids="clean", key="dead_letter_json")

    enriched_df = (
        pl.read_json(enriched_json.encode())
        if enriched_json and enriched_json != "[]"
        else pl.DataFrame()
    )
    anomalies_df = (
        pl.read_json(anomalies_json.encode())
        if anomalies_json and anomalies_json != "[]"
        else pl.DataFrame()
    )
    dead_letter_df = (
        pl.read_json(dead_letter_json.encode())
        if dead_letter_json and dead_letter_json != "[]"
        else None
    )

    load_to_db(enriched_df, anomalies_df, dead_letter_df, raw_event_ids=None)
    logger.info("load: data persisted to database.")


def mark_processed(**context: Any) -> None:
    """
    Mark all raw_events rows that were processed in this DAG run as processed=TRUE.
    """
    ti = context["ti"]
    raw_json = ti.xcom_pull(task_ids="extract", key="raw_events_json")

    if not raw_json or raw_json == "[]":
        logger.info("mark_processed: nothing to mark.")
        return

    raw_rows = json.loads(raw_json)
    ids = [r["id"] for r in raw_rows if r.get("id") is not None]

    if not ids:
        logger.info("mark_processed: no valid IDs to update.")
        return

    conn = _pg_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE raw_events SET processed = TRUE WHERE id = ANY(%(ids)s)",
                    {"ids": ids},
                )
        logger.info("mark_processed: marked %d events as processed.", len(ids))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "iot-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "start_date": datetime(2024, 1, 1, tzinfo=timezone.utc),
}

with DAG(
    dag_id="iot_etl_pipeline",
    default_args=default_args,
    description="IoT sensor ETL pipeline: raw_events → clean → enrich → anomaly → load",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["iot", "etl"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t_clean = PythonOperator(
        task_id="clean",
        python_callable=clean,
    )

    t_enrich = PythonOperator(
        task_id="enrich",
        python_callable=enrich,
    )

    t_detect_anomalies = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t_mark_processed = PythonOperator(
        task_id="mark_processed",
        python_callable=mark_processed,
    )

    # Task dependency chain
    t_extract >> t_clean >> t_enrich >> t_detect_anomalies >> t_load >> t_mark_processed
