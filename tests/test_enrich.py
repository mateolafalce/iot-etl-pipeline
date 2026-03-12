"""
Tests for dags/tasks/enrich.py
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import polars as pl
import pytest

from dags.tasks.enrich import enrich_events


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

METADATA_ROWS = [
    {
        "sensor_id": "temp-01",
        "sensor_type": "temperature",
        "location": "Server Room A",
        "zone": "zone-a",
        "min_expected": -50.0,
        "max_expected": 100.0,
        "baseline_events_per_min": 0.5,
    },
    {
        "sensor_id": "motion-01",
        "sensor_type": "motion",
        "location": "Main Entrance",
        "zone": "zone-a",
        "min_expected": 0.0,
        "max_expected": 1.0,
        "baseline_events_per_min": 2.0,
    },
    {
        "sensor_id": "door-01",
        "sensor_type": "door",
        "location": "Front Door",
        "zone": "zone-a",
        "min_expected": 0.0,
        "max_expected": 1.0,
        "baseline_events_per_min": 0.3,
    },
]

META_DF = pl.DataFrame(METADATA_ROWS)


def _clean_row(
    raw_event_id: int = 1,
    sensor_id: str = "temp-01",
    sensor_type: str = "temperature",
    value: float = 22.0,
    unit: str = "celsius",
    event_ts: str = "2024-06-03T10:30:00+00:00",  # Monday 10:30 UTC
) -> dict:
    return {
        "raw_event_id": raw_event_id,
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "value": value,
        "unit": unit,
        "event_ts": event_ts,
    }


def _df(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# hour_of_day tests
# ---------------------------------------------------------------------------

class TestHourOfDay:
    def test_hour_extracted_correctly(self):
        # 2024-06-03T14:45:00Z → hour_of_day = 14
        row = _clean_row(event_ts="2024-06-03T14:45:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["hour_of_day"][0] == 14

    def test_midnight_hour(self):
        row = _clean_row(event_ts="2024-06-03T00:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["hour_of_day"][0] == 0

    def test_end_of_day_hour(self):
        row = _clean_row(event_ts="2024-06-03T23:59:59+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["hour_of_day"][0] == 23

    def test_timezone_offset_normalised(self):
        # 2024-06-03T12:00:00+02:00 → UTC 10:00 → hour_of_day = 10
        row = _clean_row(event_ts="2024-06-03T12:00:00+02:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["hour_of_day"][0] == 10


# ---------------------------------------------------------------------------
# day_of_week tests  (0 = Monday, 6 = Sunday)
# ---------------------------------------------------------------------------

class TestDayOfWeek:
    def test_monday_is_zero(self):
        # 2024-06-03 is a Monday
        row = _clean_row(event_ts="2024-06-03T10:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["day_of_week"][0] == 0

    def test_wednesday_is_two(self):
        # 2024-06-05 is a Wednesday
        row = _clean_row(event_ts="2024-06-05T10:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["day_of_week"][0] == 2

    def test_saturday_is_five(self):
        # 2024-06-08 is a Saturday
        row = _clean_row(event_ts="2024-06-08T10:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["day_of_week"][0] == 5

    def test_sunday_is_six(self):
        # 2024-06-09 is a Sunday
        row = _clean_row(event_ts="2024-06-09T10:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["day_of_week"][0] == 6


# ---------------------------------------------------------------------------
# is_business_hours tests
# ---------------------------------------------------------------------------

class TestBusinessHours:
    def test_monday_9am_is_business_hours(self):
        row = _clean_row(event_ts="2024-06-03T09:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["is_business_hours"][0] is True

    def test_monday_8am_is_business_hours(self):
        # Boundary: hour 8 should be included
        row = _clean_row(event_ts="2024-06-03T08:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["is_business_hours"][0] is True

    def test_monday_18_00_not_business_hours(self):
        # Boundary: hour 18 should be excluded (range is 8 <= h < 18)
        row = _clean_row(event_ts="2024-06-03T18:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["is_business_hours"][0] is False

    def test_monday_7am_not_business_hours(self):
        row = _clean_row(event_ts="2024-06-03T07:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["is_business_hours"][0] is False

    def test_saturday_10am_not_business_hours(self):
        # Weekend – should always be False regardless of hour
        row = _clean_row(event_ts="2024-06-08T10:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["is_business_hours"][0] is False

    def test_sunday_12pm_not_business_hours(self):
        row = _clean_row(event_ts="2024-06-09T12:00:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["is_business_hours"][0] is False

    def test_friday_17_59_is_business_hours(self):
        # 2024-06-07 is Friday; 17:59 should be inside business hours
        row = _clean_row(event_ts="2024-06-07T17:59:00+00:00")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["is_business_hours"][0] is True


# ---------------------------------------------------------------------------
# Metadata join
# ---------------------------------------------------------------------------

class TestMetadataJoin:
    def test_zone_populated_for_known_sensor(self):
        row = _clean_row(sensor_id="temp-01")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["zone"][0] == "zone-a"

    def test_location_populated_for_known_sensor(self):
        row = _clean_row(sensor_id="temp-01")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["location"][0] == "Server Room A"

    def test_unknown_sensor_has_null_zone(self):
        row = _clean_row(sensor_id="unknown-99")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["zone"][0] is None

    def test_baseline_events_per_min_populated(self):
        row = _clean_row(sensor_id="motion-01")
        enriched = enrich_events(_df(row), META_DF)
        assert enriched["baseline_events_per_min"][0] == 2.0

    def test_multiple_sensors_joined_correctly(self):
        rows = [
            _clean_row(raw_event_id=1, sensor_id="temp-01", event_ts="2024-06-03T09:00:00+00:00"),
            _clean_row(raw_event_id=2, sensor_id="motion-01", event_ts="2024-06-03T09:00:00+00:00"),
            _clean_row(raw_event_id=3, sensor_id="door-01", event_ts="2024-06-03T09:00:00+00:00"),
        ]
        enriched = enrich_events(_df(*rows), META_DF)
        zones = enriched["zone"].to_list()
        assert all(z == "zone-a" for z in zones)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEnrichEdgeCases:
    def test_empty_input_returns_empty(self):
        empty = pl.DataFrame(
            schema={
                "raw_event_id": pl.Int64,
                "sensor_id": pl.Utf8,
                "sensor_type": pl.Utf8,
                "value": pl.Float64,
                "unit": pl.Utf8,
                "event_ts": pl.Utf8,
            }
        )
        enriched = enrich_events(empty, META_DF)
        assert enriched.height == 0

    def test_empty_metadata_produces_null_zone(self):
        row = _clean_row()
        empty_meta = pl.DataFrame(
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
        enriched = enrich_events(_df(row), empty_meta)
        assert enriched.height == 1
        assert enriched["zone"][0] is None

    def test_output_contains_all_required_columns(self):
        row = _clean_row()
        enriched = enrich_events(_df(row), META_DF)
        required = {
            "sensor_id",
            "sensor_type",
            "value",
            "unit",
            "event_ts",
            "hour_of_day",
            "day_of_week",
            "is_business_hours",
            "zone",
            "location",
        }
        assert required.issubset(set(enriched.columns))
