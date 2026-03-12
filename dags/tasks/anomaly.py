"""
Anomaly Detection Step
======================
Applies rule-based and statistical anomaly detection to enriched IoT events.

Detection strategies
--------------------
Temperature sensors
    Z-score over a 1-hour rolling window per sensor.
    An event is flagged when |z| > 3.

Motion sensors
    Count events per minute per sensor and compare against the
    ``baseline_events_per_min`` value from sensor_metadata.
    Flagged when the per-minute count exceeds 3x the baseline.

Door sensors
    Flag any "open" event (value == 1) that occurs outside business hours
    (is_business_hours == False).

Returns
-------
pl.DataFrame
    Columns: ``sensor_id``, ``sensor_type``, ``event_ts``, ``value``,
    ``anomaly_type``, ``score``, ``reason``.
"""

from __future__ import annotations

import logging
from typing import List

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# Z-score threshold for temperature anomalies
ZSCORE_THRESHOLD = 3.0

# Motion: flag when event count per minute exceeds this multiplier x baseline
MOTION_BASELINE_MULTIPLIER = 3.0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _temperature_anomalies(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect temperature anomalies using a per-sensor z-score.

    We use a 1-hour rolling window per sensor.  Because Polars rolling
    group-by on an arbitrary time column requires sorted data we sort
    explicitly, compute z-scores in Python/NumPy per sensor group, and
    assemble the result.
    """
    temp_df = df.filter(pl.col("sensor_type") == "temperature")
    if temp_df.is_empty():
        return _empty_anomaly_df()

    # Parse event_ts to datetime for sorting
    temp_df = temp_df.with_columns(
        pl.col("event_ts")
        .str.strptime(pl.Datetime("us", "UTC"), format=None, strict=False)
        .alias("event_ts_dt")
    ).sort(["sensor_id", "event_ts_dt"])

    anomaly_records: List[dict] = []

    for sensor_id in temp_df["sensor_id"].unique().to_list():
        sensor_data = temp_df.filter(pl.col("sensor_id") == sensor_id)
        values = sensor_data["value"].to_numpy().astype(float)

        if len(values) < 2:
            continue

        # Rolling 1-hour window: for each point collect all events within
        # the preceding 3600 seconds and compute z-score.
        ts_series = sensor_data["event_ts_dt"].to_list()

        for i in range(len(values)):
            current_ts = ts_series[i]
            # Gather window: all previous + current within 1 hour
            window_vals = []
            for j in range(i + 1):
                diff_secs = (current_ts - ts_series[j]).total_seconds()
                if 0 <= diff_secs <= 3600:
                    window_vals.append(values[j])

            if len(window_vals) < 2:
                continue

            arr = np.array(window_vals, dtype=float)
            mean = arr.mean()
            std = arr.std()
            if std == 0:
                continue

            z = (values[i] - mean) / std
            if abs(z) > ZSCORE_THRESHOLD:
                row = sensor_data[i].to_dicts()[0]
                anomaly_records.append(
                    {
                        "sensor_id": row["sensor_id"],
                        "sensor_type": row["sensor_type"],
                        "event_ts": row["event_ts"],
                        "value": float(values[i]),
                        "anomaly_type": "temperature_zscore",
                        "score": round(float(abs(z)), 4),
                        "reason": (
                            f"Z-score {z:.2f} exceeds threshold +-{ZSCORE_THRESHOLD} "
                            f"(window mean={mean:.2f}, std={std:.2f})"
                        ),
                    }
                )

    if not anomaly_records:
        return _empty_anomaly_df()

    return pl.DataFrame(anomaly_records)


def detect_temperature_anomalies(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect temperature anomalies using a per-sensor z-score.

    Parameters
    ----------
    df:
        Enriched DataFrame (must include temperature sensor rows).

    Returns
    -------
    pl.DataFrame with anomaly rows. anomaly_type='temperature_spike', score=z_score,
    reason='Z-score exceeded threshold'.
    """
    return _temperature_anomalies(df)


def _motion_anomalies(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect abnormal motion event bursts.

    Groups events by (sensor_id, truncated-to-minute) and flags minutes
    where the count exceeds MOTION_BASELINE_MULTIPLIER x baseline.
    """
    motion_df = df.filter(pl.col("sensor_type") == "motion")
    if motion_df.is_empty():
        return _empty_anomaly_df()

    # We need baseline_events_per_min; drop rows where it is null
    motion_df = motion_df.filter(pl.col("baseline_events_per_min").is_not_null())
    if motion_df.is_empty():
        return _empty_anomaly_df()

    motion_df = motion_df.with_columns(
        pl.col("event_ts")
        .str.strptime(pl.Datetime("us", "UTC"), format=None, strict=False)
        .alias("event_ts_dt")
    )

    # Truncate to minute bucket
    motion_df = motion_df.with_columns(
        pl.col("event_ts_dt").dt.truncate("1m").alias("minute_bucket")
    )

    # Per-sensor-per-minute count
    counts = (
        motion_df.group_by(["sensor_id", "sensor_type", "minute_bucket"])
        .agg(
            [
                pl.len().alias("event_count"),
                pl.col("baseline_events_per_min").first(),
                pl.col("event_ts").first(),
                pl.col("value").mean().alias("value"),
            ]
        )
    )

    threshold_series = (
        counts["baseline_events_per_min"] * MOTION_BASELINE_MULTIPLIER
    )
    flagged = counts.filter(
        pl.col("event_count") > threshold_series
    )

    if flagged.is_empty():
        return _empty_anomaly_df()

    return flagged.select(
        [
            pl.col("sensor_id"),
            pl.col("sensor_type"),
            pl.col("event_ts"),
            pl.col("value").cast(pl.Float64),
            pl.lit("motion_burst").alias("anomaly_type"),
            (pl.col("event_count") / pl.col("baseline_events_per_min"))
            .round(4)
            .alias("score"),
            (
                pl.lit("Events per minute (")
                + pl.col("event_count").cast(pl.Utf8)
                + pl.lit(") exceeds ")
                + (pl.col("baseline_events_per_min") * MOTION_BASELINE_MULTIPLIER)
                    .round(1)
                    .cast(pl.Utf8)
                + pl.lit("x baseline")
            ).alias("reason"),
        ]
    )


def detect_motion_anomalies(df: pl.DataFrame, metadata_df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect abnormal motion event bursts.

    Parameters
    ----------
    df:
        Enriched DataFrame (should already contain baseline_events_per_min from join).
    metadata_df:
        Sensor metadata DataFrame (used if baseline_events_per_min not in df).

    Returns
    -------
    pl.DataFrame with anomaly rows. anomaly_type='motion_burst', score=ratio.
    """
    # If df doesn't have baseline_events_per_min, join it from metadata
    if "baseline_events_per_min" not in df.columns and not metadata_df.is_empty():
        meta = metadata_df.select(["sensor_id", "baseline_events_per_min"])
        df = df.join(meta, on="sensor_id", how="left")

    return _motion_anomalies(df)


def _door_anomalies(df: pl.DataFrame) -> pl.DataFrame:
    """Detect door-open events that occur outside business hours."""
    door_df = df.filter(
        (pl.col("sensor_type") == "door")
        & (pl.col("value") == 1.0)
        & (pl.col("is_business_hours") == False)  # noqa: E712
    )

    if door_df.is_empty():
        return _empty_anomaly_df()

    return door_df.select(
        [
            pl.col("sensor_id"),
            pl.col("sensor_type"),
            pl.col("event_ts"),
            pl.col("value").cast(pl.Float64),
            pl.lit("door_outside_hours").alias("anomaly_type"),
            pl.lit(1.0).cast(pl.Float64).alias("score"),
            pl.lit("Door opened outside business hours (Mon-Fri 08:00-18:00)").alias(
                "reason"
            ),
        ]
    )


def detect_door_anomalies(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect door-open events that occur outside business hours.

    Parameters
    ----------
    df:
        Enriched DataFrame with is_business_hours column.

    Returns
    -------
    pl.DataFrame with anomaly rows. anomaly_type='after_hours_door', score=1.0.
    """
    return _door_anomalies(df)


def _empty_anomaly_df() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "sensor_id": pl.Utf8,
            "sensor_type": pl.Utf8,
            "event_ts": pl.Utf8,
            "value": pl.Float64,
            "anomaly_type": pl.Utf8,
            "score": pl.Float64,
            "reason": pl.Utf8,
        }
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_anomalies(enriched_df: pl.DataFrame) -> pl.DataFrame:
    """
    Run all anomaly detectors against the enriched events DataFrame.

    Parameters
    ----------
    enriched_df:
        Output of :func:`~dags.tasks.enrich.enrich_events`.

    Returns
    -------
    pl.DataFrame
        All detected anomalies with columns:
        ``sensor_id``, ``sensor_type``, ``event_ts``, ``value``,
        ``anomaly_type``, ``score``, ``reason``.
    """
    if enriched_df.is_empty():
        logger.info("detect_anomalies: input is empty.")
        return _empty_anomaly_df()

    logger.info("detect_anomalies: scanning %d enriched events.", enriched_df.height)

    parts = [
        _temperature_anomalies(enriched_df),
        _motion_anomalies(enriched_df),
        _door_anomalies(enriched_df),
    ]

    # Filter out empty parts before concat to avoid schema mismatch warnings
    non_empty = [p for p in parts if not p.is_empty()]
    if not non_empty:
        logger.info("detect_anomalies: no anomalies detected.")
        return _empty_anomaly_df()

    result = pl.concat(non_empty, how="diagonal")
    logger.info("detect_anomalies: detected %d anomalies.", result.height)
    return result


def run_anomaly_detection(**kwargs) -> None:
    """
    Airflow task callable: run all anomaly detectors.

    1. Pull enriched_records_json from XCom (produced by run_enrich).
    2. Deserialise to a Polars DataFrame.
    3. Run all detectors.
    4. Push anomaly records via XCom for the load step.
    """
    ti = kwargs.get("ti")

    enriched_json = None
    if ti:
        enriched_json = ti.xcom_pull(task_ids="enrich", key="enriched_records_json")

    if not enriched_json or enriched_json == "[]":
        logger.info("run_anomaly_detection: no enriched data to analyse.")
        if ti:
            ti.xcom_push(key="anomaly_records_json", value="[]")
        return

    enriched_df = pl.read_json(enriched_json.encode())
    anomalies_df = detect_anomalies(enriched_df)

    logger.info(
        "run_anomaly_detection complete: %d anomalies detected.", anomalies_df.height
    )

    if ti:
        ti.xcom_push(key="anomaly_records_json", value=anomalies_df.write_json())
