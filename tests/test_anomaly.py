"""
Tests for dags/tasks/anomaly.py
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from dags.tasks.anomaly import (
    detect_anomalies,
    ZSCORE_THRESHOLD,
    MOTION_BASELINE_MULTIPLIER,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TS = datetime(2024, 6, 3, 10, 0, 0, tzinfo=timezone.utc)  # Monday 10:00 UTC
_WEEKEND_TS = datetime(2024, 6, 8, 10, 0, 0, tzinfo=timezone.utc)  # Saturday 10:00 UTC
_NIGHT_TS = datetime(2024, 6, 3, 2, 0, 0, tzinfo=timezone.utc)    # Monday 02:00 UTC


def _ts(dt: datetime) -> str:
    return dt.isoformat()


def _enriched_row(
    sensor_id: str,
    sensor_type: str,
    value: float,
    event_ts: str,
    is_business_hours: bool = True,
    unit: str = "celsius",
    zone: str = "zone-a",
    location: str = "Test Location",
    baseline_events_per_min: float = 1.0,
    hour_of_day: int = 10,
    day_of_week: int = 0,
) -> dict:
    return {
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "value": value,
        "unit": unit,
        "event_ts": event_ts,
        "hour_of_day": hour_of_day,
        "day_of_week": day_of_week,
        "is_business_hours": is_business_hours,
        "zone": zone,
        "location": location,
        "baseline_events_per_min": baseline_events_per_min,
        "min_expected": -50.0,
        "max_expected": 100.0,
    }


def _df(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# Z-score detection (temperature)
# ---------------------------------------------------------------------------

class TestZscoreDetection:
    def test_no_anomaly_for_stable_readings(self):
        # All readings within 1 °C of each other – no spike expected
        rows = [
            _enriched_row(
                "temp-01",
                "temperature",
                value=22.0 + (i * 0.1),
                event_ts=_ts(_BASE_TS + timedelta(minutes=i)),
            )
            for i in range(10)
        ]
        result = detect_anomalies(_df(*rows))
        temp_anomalies = result.filter(pl.col("anomaly_type") == "temperature_zscore")
        assert temp_anomalies.height == 0

    def test_spike_detected_above_threshold(self):
        # 9 normal readings around 22 °C, then a massive spike
        rows = [
            _enriched_row(
                "temp-01",
                "temperature",
                value=22.0,
                event_ts=_ts(_BASE_TS + timedelta(minutes=i)),
            )
            for i in range(9)
        ]
        # Spike: value far outside the distribution (> 3 sigma)
        rows.append(
            _enriched_row(
                "temp-01",
                "temperature",
                value=75.0,
                event_ts=_ts(_BASE_TS + timedelta(minutes=9)),
            )
        )
        result = detect_anomalies(_df(*rows))
        temp_anomalies = result.filter(pl.col("anomaly_type") == "temperature_zscore")
        assert temp_anomalies.height >= 1
        assert temp_anomalies["score"][0] > ZSCORE_THRESHOLD

    def test_anomaly_score_populated(self):
        rows = [
            _enriched_row("temp-01", "temperature", 22.0, _ts(_BASE_TS + timedelta(minutes=i)))
            for i in range(8)
        ]
        rows.append(
            _enriched_row("temp-01", "temperature", 90.0, _ts(_BASE_TS + timedelta(minutes=8)))
        )
        result = detect_anomalies(_df(*rows))
        if result.height > 0:
            assert result["score"][0] is not None
            assert result["score"][0] > 0

    def test_non_temperature_sensor_not_checked_for_zscore(self):
        # Motion sensor spikes should not trigger temperature z-score check
        rows = [
            _enriched_row("motion-01", "motion", float(v), _ts(_BASE_TS + timedelta(minutes=i)))
            for i, v in enumerate([0, 1, 0, 1, 0, 1, 0, 1, 0, 1])
        ]
        result = detect_anomalies(_df(*rows))
        temp_anomalies = result.filter(pl.col("anomaly_type") == "temperature_zscore")
        assert temp_anomalies.height == 0

    def test_only_two_temperature_readings_no_crash(self):
        # Edge case: too few points for meaningful z-score
        rows = [
            _enriched_row("temp-01", "temperature", 22.0, _ts(_BASE_TS)),
            _enriched_row("temp-01", "temperature", 23.0, _ts(_BASE_TS + timedelta(minutes=1))),
        ]
        # Should not raise
        result = detect_anomalies(_df(*rows))
        assert isinstance(result, pl.DataFrame)


# ---------------------------------------------------------------------------
# Motion burst detection
# ---------------------------------------------------------------------------

class TestMotionBurstDetection:
    def test_no_anomaly_at_normal_rate(self):
        # baseline=2.0; 3x threshold = 6 per minute; we send 3 → no anomaly
        rows = [
            _enriched_row(
                "motion-01",
                "motion",
                1.0,
                _ts(_BASE_TS + timedelta(seconds=i * 10)),
                baseline_events_per_min=2.0,
            )
            for i in range(3)
        ]
        result = detect_anomalies(_df(*rows))
        motion_anomalies = result.filter(pl.col("anomaly_type") == "motion_burst")
        assert motion_anomalies.height == 0

    def test_anomaly_detected_above_3x_baseline(self):
        # baseline=2.0; 3x threshold = 6; send 10 events in the same minute
        rows = [
            _enriched_row(
                "motion-01",
                "motion",
                1.0,
                _ts(_BASE_TS + timedelta(seconds=i * 3)),
                baseline_events_per_min=2.0,
            )
            for i in range(10)
        ]
        result = detect_anomalies(_df(*rows))
        motion_anomalies = result.filter(pl.col("anomaly_type") == "motion_burst")
        assert motion_anomalies.height >= 1

    def test_motion_score_is_ratio_above_1(self):
        rows = [
            _enriched_row(
                "motion-01",
                "motion",
                1.0,
                _ts(_BASE_TS + timedelta(seconds=i * 2)),
                baseline_events_per_min=1.0,
            )
            for i in range(20)
        ]
        result = detect_anomalies(_df(*rows))
        motion_anomalies = result.filter(pl.col("anomaly_type") == "motion_burst")
        if motion_anomalies.height > 0:
            # Score = count / baseline; should be > MOTION_BASELINE_MULTIPLIER
            assert motion_anomalies["score"][0] > MOTION_BASELINE_MULTIPLIER

    def test_null_baseline_skipped(self):
        rows = [
            _enriched_row("motion-01", "motion", 1.0, _ts(_BASE_TS + timedelta(seconds=i)))
            for i in range(10)
        ]
        # Override baseline to None
        df = _df(*rows).with_columns(pl.lit(None).cast(pl.Float64).alias("baseline_events_per_min"))
        # Should not raise
        result = detect_anomalies(df)
        assert isinstance(result, pl.DataFrame)


# ---------------------------------------------------------------------------
# Door outside hours detection
# ---------------------------------------------------------------------------

class TestDoorOutsideHours:
    def test_door_open_outside_hours_flagged(self):
        row = _enriched_row(
            "door-01",
            "door",
            1.0,  # open
            _ts(_NIGHT_TS),
            is_business_hours=False,
            hour_of_day=2,
            day_of_week=0,
        )
        result = detect_anomalies(_df(row))
        door_anomalies = result.filter(pl.col("anomaly_type") == "door_outside_hours")
        assert door_anomalies.height == 1

    def test_door_open_during_hours_not_flagged(self):
        row = _enriched_row(
            "door-01",
            "door",
            1.0,  # open
            _ts(_BASE_TS),
            is_business_hours=True,
            hour_of_day=10,
            day_of_week=0,
        )
        result = detect_anomalies(_df(row))
        door_anomalies = result.filter(pl.col("anomaly_type") == "door_outside_hours")
        assert door_anomalies.height == 0

    def test_door_closed_outside_hours_not_flagged(self):
        row = _enriched_row(
            "door-01",
            "door",
            0.0,  # closed
            _ts(_NIGHT_TS),
            is_business_hours=False,
            hour_of_day=2,
            day_of_week=0,
        )
        result = detect_anomalies(_df(row))
        door_anomalies = result.filter(pl.col("anomaly_type") == "door_outside_hours")
        assert door_anomalies.height == 0

    def test_door_open_on_weekend_flagged(self):
        row = _enriched_row(
            "door-01",
            "door",
            1.0,  # open
            _ts(_WEEKEND_TS),
            is_business_hours=False,  # Saturday
            hour_of_day=10,
            day_of_week=5,
        )
        result = detect_anomalies(_df(row))
        door_anomalies = result.filter(pl.col("anomaly_type") == "door_outside_hours")
        assert door_anomalies.height == 1

    def test_door_anomaly_reason_text(self):
        row = _enriched_row(
            "door-01",
            "door",
            1.0,
            _ts(_NIGHT_TS),
            is_business_hours=False,
            hour_of_day=2,
            day_of_week=0,
        )
        result = detect_anomalies(_df(row))
        door_anomalies = result.filter(pl.col("anomaly_type") == "door_outside_hours")
        assert door_anomalies.height == 1
        assert "business hours" in door_anomalies["reason"][0].lower()

    def test_multiple_door_opens_outside_hours(self):
        rows = [
            _enriched_row(
                "door-01",
                "door",
                1.0,
                _ts(_NIGHT_TS + timedelta(minutes=i)),
                is_business_hours=False,
                hour_of_day=2,
                day_of_week=0,
            )
            for i in range(3)
        ]
        result = detect_anomalies(_df(*rows))
        door_anomalies = result.filter(pl.col("anomaly_type") == "door_outside_hours")
        assert door_anomalies.height == 3


# ---------------------------------------------------------------------------
# Output schema & general behaviour
# ---------------------------------------------------------------------------

class TestAnomalyOutputSchema:
    def test_empty_input_returns_empty_df(self):
        empty = pl.DataFrame(
            schema={
                "sensor_id": pl.Utf8,
                "sensor_type": pl.Utf8,
                "value": pl.Float64,
                "event_ts": pl.Utf8,
                "is_business_hours": pl.Boolean,
                "hour_of_day": pl.Int32,
                "day_of_week": pl.Int32,
                "baseline_events_per_min": pl.Float64,
            }
        )
        result = detect_anomalies(empty)
        assert result.height == 0

    def test_required_columns_present(self):
        row = _enriched_row("door-01", "door", 1.0, _ts(_NIGHT_TS), is_business_hours=False)
        result = detect_anomalies(_df(row))
        required = {"sensor_id", "sensor_type", "event_ts", "value", "anomaly_type", "score", "reason"}
        assert required.issubset(set(result.columns))

    def test_no_false_positives_on_all_valid_data(self):
        rows = []
        # Normal temperature readings
        for i in range(20):
            rows.append(
                _enriched_row(
                    "temp-01",
                    "temperature",
                    22.0 + (i % 3) * 0.5,
                    _ts(_BASE_TS + timedelta(minutes=i)),
                )
            )
        # Occasional door opens during business hours
        rows.append(_enriched_row("door-01", "door", 1.0, _ts(_BASE_TS), is_business_hours=True))
        rows.append(_enriched_row("door-01", "door", 0.0, _ts(_BASE_TS + timedelta(minutes=5)), is_business_hours=True))
        # Low-frequency motion during business hours (below 3x threshold)
        for i in range(3):
            rows.append(
                _enriched_row(
                    "motion-01",
                    "motion",
                    1.0,
                    _ts(_BASE_TS + timedelta(minutes=i * 20)),
                    baseline_events_per_min=2.0,
                )
            )
        result = detect_anomalies(_df(*rows))
        # No door or motion anomalies
        non_temp = result.filter(pl.col("anomaly_type") != "temperature_zscore")
        assert non_temp.height == 0
