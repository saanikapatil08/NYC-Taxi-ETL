"""Quality-suite tests on the dataframe-level checks."""

from __future__ import annotations

import pandas as pd
import pytest

from shared.quality_checks import (
    QualityCheckFailure,
    assert_suite_passed,
    check_distinct_zone_count,
    check_no_nulls_required,
    check_pickup_before_dropoff,
    check_positive_fare,
    run_dataframe_suite,
)
from shared.transform import clean_trips


def test_dataframe_suite_passes_on_clean(raw_frame: pd.DataFrame) -> None:
    cleaned = clean_trips(raw_frame, year=2024, month=3)
    suite = run_dataframe_suite(cleaned)
    # Distinct zones is a soft warn; the synthetic frame uses one zone pair, so
    # the warn check is allowed to fail without failing the suite.
    assert suite.passed
    assert all(r.severity in {"error", "warn"} for r in suite.results)


def test_assert_suite_passed_raises_on_error(raw_frame: pd.DataFrame) -> None:
    cleaned = clean_trips(raw_frame, year=2024, month=3)
    cleaned.loc[cleaned.index[0], "fare_amount"] = -10.0
    suite = run_dataframe_suite(cleaned)
    with pytest.raises(QualityCheckFailure):
        assert_suite_passed(suite, label="dataframe")


def test_pickup_before_dropoff_detects_inversion() -> None:
    bad = pd.DataFrame(
        [
            {
                "vendor_id": 1,
                "pickup_ts": pd.Timestamp("2024-03-01 10:00"),
                "dropoff_ts": pd.Timestamp("2024-03-01 09:00"),
                "passenger_count": 1,
                "trip_distance_mi": 1.0,
                "trip_duration_min": 30.0,
                "pickup_zone_id": 1,
                "dropoff_zone_id": 2,
                "payment_type": 1,
                "fare_amount": 10.0,
                "tip_amount": 0.0,
                "total_amount": 10.0,
                "trip_year": 2024,
                "trip_month": 3,
            }
        ]
    )
    res = check_pickup_before_dropoff(bad)
    assert not res.passed and res.metric == 1


def test_positive_fare_detects_negative() -> None:
    df = pd.DataFrame({"fare_amount": [10.0, -1.0, 5.0]})
    res = check_positive_fare(df)
    assert not res.passed and res.metric == 1


def test_no_nulls_required_detects_missing() -> None:
    df = pd.DataFrame(
        [
            {
                "vendor_id": 1,
                "pickup_ts": pd.Timestamp("2024-03-01"),
                "dropoff_ts": pd.Timestamp("2024-03-01"),
                "pickup_zone_id": 1,
                "dropoff_zone_id": None,
                "fare_amount": 5.0,
                "total_amount": 6.0,
            }
        ]
    )
    res = check_no_nulls_required(df)
    assert not res.passed and res.metric == 1


def test_distinct_zone_count_warns_when_low() -> None:
    df = pd.DataFrame({"pickup_zone_id": [1, 1, 1, 2]})
    res = check_distinct_zone_count(df, min_zones=10)
    assert not res.passed and res.severity == "warn"
