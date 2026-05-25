"""Transform tests for shared.transform.clean_trips."""

from __future__ import annotations

import pandas as pd

from shared.transform import clean_trips


def test_clean_trips_drops_invalid_rows(raw_frame: pd.DataFrame) -> None:
    cleaned = clean_trips(raw_frame, year=2024, month=3)
    # Zero-distance, negative-fare, and out-of-partition rows must be dropped.
    assert (cleaned["trip_distance_mi"] > 0).all()
    assert (cleaned["fare_amount"] >= 0).all()
    assert (cleaned["trip_year"] == 2024).all()
    assert (cleaned["trip_month"] == 3).all()
    assert len(cleaned) <= len(raw_frame) - 3


def test_clean_trips_preserves_required_columns(raw_frame: pd.DataFrame) -> None:
    cleaned = clean_trips(raw_frame, year=2024, month=3)
    expected = {
        "vendor_id",
        "pickup_ts",
        "dropoff_ts",
        "passenger_count",
        "trip_distance_mi",
        "trip_duration_min",
        "pickup_zone_id",
        "dropoff_zone_id",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "trip_year",
        "trip_month",
    }
    assert expected.issubset(cleaned.columns)


def test_clean_trips_handles_empty_frame() -> None:
    cleaned = clean_trips(pd.DataFrame(), year=2024, month=3)
    assert cleaned.empty


def test_clean_trips_drops_unbounded_durations() -> None:
    # A single 12-hour trip should be filtered (MAX_TRIP_HOURS = 6).
    df = pd.DataFrame(
        [
            {
                "VendorID": 1,
                "tpep_pickup_datetime": pd.Timestamp("2024-03-01 06:00"),
                "tpep_dropoff_datetime": pd.Timestamp("2024-03-01 18:00"),
                "passenger_count": 1,
                "trip_distance": 5.0,
                "PULocationID": 1,
                "DOLocationID": 2,
                "payment_type": 1,
                "fare_amount": 30.0,
                "tip_amount": 0.0,
                "total_amount": 32.0,
            }
        ]
    )
    cleaned = clean_trips(df, year=2024, month=3)
    assert cleaned.empty
