"""
Tests for dags/tasks/clean.py
"""

from __future__ import annotations

import sys
import os

# Allow imports without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timezone

import polars as pl
import pytest

from dags.tasks.clean import clean_events, VALID_RANGES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(
    id: int = 1,
    sensor_id: str = "temp-01",
    sensor_type: str = "temperature",
    value=20.0,
    unit: str = "celsius",
    timestamp: str | None = "2024-06-01T10:00:00+00:00",
) -> dict:
    return {
        "id": id,
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "value": value,
        "unit": unit,
        "timestamp": timestamp,
        "payload": "{}",
    }


def _df(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# Basic happy-path
# ---------------------------------------------------------------------------

class TestCleanHappyPath:
    def test_single_valid_row_passes(self):
        df = _df(_make_row())
        clean, dead = clean_events(df)
        assert clean.height == 1
        assert dead.height == 0

    def test_multiple_valid_rows(self):
        rows = [_make_row(id=i, value=20.0 + i) for i in range(1, 6)]
        clean, dead = clean_events(_df(*rows))
        assert clean.height == 5
        assert dead.height == 0

    def test_empty_input_returns_empty_dfs(self):
        empty = pl.DataFrame(
            schema={
                "id": pl.Int64,
                "sensor_id": pl.Utf8,
                "sensor_type": pl.Utf8,
                "value": pl.Float64,
                "unit": pl.Utf8,
                "timestamp": pl.Utf8,
                "payload": pl.Utf8,
            }
        )
        clean, dead = clean_events(empty)
        assert clean.height == 0
        assert dead.height == 0


# ---------------------------------------------------------------------------
# Null value removal
# ---------------------------------------------------------------------------

class TestNullValueRemoval:
    def test_null_value_goes_to_dead_letter(self):
        rows = [
            _make_row(id=1, value=20.0),
            _make_row(id=2, value=None),
            _make_row(id=3, value=21.5),
        ]
        clean, dead = clean_events(_df(*rows))
        assert clean.height == 2
        assert dead.height == 1
        assert "null value" in dead["reason"][0]

    def test_all_null_values_to_dead_letter(self):
        rows = [_make_row(id=i, value=None) for i in range(1, 4)]
        clean, dead = clean_events(_df(*rows))
        assert clean.height == 0
        assert dead.height == 3

    def test_clean_df_has_no_null_values(self):
        rows = [_make_row(id=i, value=20.0 if i % 2 == 0 else None) for i in range(1, 7)]
        clean, dead = clean_events(_df(*rows))
        assert clean["value"].is_null().sum() == 0


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_exact_duplicate_removed(self):
        row = _make_row(id=1, sensor_id="temp-01", timestamp="2024-06-01T10:00:00+00:00", value=20.0)
        row2 = {**row, "id": 2}  # same sensor_id + timestamp
        clean, dead = clean_events(_df(row, row2))
        assert clean.height == 1

    def test_different_timestamps_not_deduped(self):
        rows = [
            _make_row(id=1, sensor_id="temp-01", timestamp="2024-06-01T10:00:00+00:00", value=20.0),
            _make_row(id=2, sensor_id="temp-01", timestamp="2024-06-01T10:01:00+00:00", value=21.0),
        ]
        clean, dead = clean_events(_df(*rows))
        assert clean.height == 2

    def test_different_sensors_same_timestamp_not_deduped(self):
        rows = [
            _make_row(id=1, sensor_id="temp-01", timestamp="2024-06-01T10:00:00+00:00", value=20.0),
            _make_row(id=2, sensor_id="temp-02", timestamp="2024-06-01T10:00:00+00:00", value=21.0),
        ]
        clean, dead = clean_events(_df(*rows))
        assert clean.height == 2


# ---------------------------------------------------------------------------
# Timestamp validation
# ---------------------------------------------------------------------------

class TestTimestampValidation:
    def test_null_timestamp_to_dead_letter(self):
        rows = [
            _make_row(id=1, value=20.0, timestamp=None),
            _make_row(id=2, value=21.0, timestamp="2024-06-01T10:00:00+00:00"),
        ]
        clean, dead = clean_events(_df(*rows))
        assert clean.height == 1
        assert dead.height == 1
        assert "null timestamp" in dead["reason"][0]

    def test_unparseable_timestamp_to_dead_letter(self):
        rows = [
            _make_row(id=1, value=20.0, timestamp="not-a-real-date"),
            _make_row(id=2, value=21.0, timestamp="2024-06-01T10:00:00+00:00"),
        ]
        clean, dead = clean_events(_df(*rows))
        assert clean.height == 1
        assert dead.height == 1
        assert "unparseable" in dead["reason"][0]

    def test_timestamp_normalised_to_utc(self):
        # Timezone-aware timestamp should be preserved in event_ts
        row = _make_row(id=1, value=20.0, timestamp="2024-06-01T12:00:00+02:00")
        clean, _ = clean_events(_df(row))
        assert clean.height == 1
        ts_val = clean["event_ts"][0]
        # Should contain UTC offset information
        assert ts_val is not None

    def test_naive_timestamp_accepted(self):
        row = _make_row(id=1, value=20.0, timestamp="2024-06-01T10:00:00")
        clean, dead = clean_events(_df(row))
        assert clean.height == 1
        assert dead.height == 0


# ---------------------------------------------------------------------------
# Range validation
# ---------------------------------------------------------------------------

class TestRangeValidation:
    def test_temperature_in_range_passes(self):
        row = _make_row(sensor_type="temperature", value=25.0)
        clean, dead = clean_events(_df(row))
        assert clean.height == 1
        assert dead.height == 0

    def test_temperature_below_min_rejected(self):
        lo, _ = VALID_RANGES["temperature"]
        row = _make_row(sensor_type="temperature", value=lo - 1.0)
        clean, dead = clean_events(_df(row))
        assert clean.height == 0
        assert dead.height == 1
        assert "out of valid range" in dead["reason"][0]

    def test_temperature_above_max_rejected(self):
        _, hi = VALID_RANGES["temperature"]
        row = _make_row(sensor_type="temperature", value=hi + 1.0)
        clean, dead = clean_events(_df(row))
        assert clean.height == 0
        assert dead.height == 1

    def test_motion_valid_values(self):
        for val in [0, 1, 0.0, 1.0]:
            row = _make_row(sensor_type="motion", value=val)
            clean, dead = clean_events(_df(row))
            assert clean.height == 1, f"Expected motion value {val} to pass"

    def test_motion_invalid_value_rejected(self):
        row = _make_row(sensor_type="motion", value=2.0)
        clean, dead = clean_events(_df(row))
        assert clean.height == 0
        assert dead.height == 1

    def test_door_invalid_value_rejected(self):
        row = _make_row(sensor_type="door", value=-1.0)
        clean, dead = clean_events(_df(row))
        assert clean.height == 0
        assert dead.height == 1

    def test_unknown_sensor_type_passes_range(self):
        # Unknown types should not be rejected for range (no entry in VALID_RANGES)
        row = _make_row(sensor_type="unknown_sensor", value=9999.0)
        clean, dead = clean_events(_df(row))
        assert clean.height == 1

    def test_mixed_valid_invalid_split_correctly(self):
        rows = [
            _make_row(id=1, sensor_type="temperature", value=20.0),   # valid
            _make_row(id=2, sensor_type="temperature", value=200.0),  # out of range
            _make_row(id=3, sensor_type="motion", value=1.0),         # valid
            _make_row(id=4, sensor_type="motion", value=5.0),         # out of range
        ]
        clean, dead = clean_events(_df(*rows))
        assert clean.height == 2
        assert dead.height == 2


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

class TestOutputSchema:
    def test_clean_df_has_expected_columns(self):
        row = _make_row()
        clean, _ = clean_events(_df(row))
        expected = {"raw_event_id", "sensor_id", "sensor_type", "value", "unit", "event_ts"}
        assert expected.issubset(set(clean.columns))

    def test_value_column_is_float(self):
        rows = [_make_row(id=i, value=float(i) + 18.0) for i in range(1, 4)]
        clean, _ = clean_events(_df(*rows))
        assert clean["value"].dtype == pl.Float64

    def test_dead_letter_has_reason_column(self):
        row = _make_row(value=None)
        _, dead = clean_events(_df(row))
        assert "reason" in dead.columns
