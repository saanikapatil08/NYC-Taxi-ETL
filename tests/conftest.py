"""Shared pytest fixtures and a synthetic NYC Taxi raw frame builder."""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

# Make sure the shared module reads sane defaults during tests.
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("ROW_COUNT_DEVIATION_PCT", "0.30")
os.environ.setdefault("MIN_EXPECTED_ROWS", "10")


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> Iterator[None]:
    """Force shared.config.get_settings to re-read env between tests."""
    from shared.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _build_raw_row(pickup: datetime, *, distance: float, fare: float, total: float) -> dict:
    return {
        "VendorID": 1,
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + timedelta(minutes=15),
        "passenger_count": 1,
        "trip_distance": distance,
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": 132,
        "DOLocationID": 230,
        "payment_type": 1,
        "fare_amount": fare,
        "extra": 0.5,
        "mta_tax": 0.5,
        "tip_amount": 1.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "total_amount": total,
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
    }


@pytest.fixture
def raw_frame() -> pd.DataFrame:
    rng = np.random.default_rng(seed=42)
    rows = []
    for _ in range(200):
        pickup = datetime(2024, 3, 15, 8, 0) + timedelta(minutes=int(rng.integers(0, 60 * 24 * 14)))
        rows.append(
            _build_raw_row(
                pickup,
                distance=float(rng.uniform(0.5, 12.0)),
                fare=float(rng.uniform(5, 60)),
                total=float(rng.uniform(7, 80)),
            )
        )

    # Inject a few intentionally-bad rows so cleaning has work to do.
    bad_pickup = datetime(2024, 3, 1, 12, 0)
    rows.append(_build_raw_row(bad_pickup, distance=0.0, fare=10.0, total=12.0))
    rows.append(_build_raw_row(bad_pickup, distance=5.0, fare=-3.0, total=12.0))
    # Out-of-partition row (April)
    rows.append(_build_raw_row(datetime(2024, 4, 5, 9, 0), distance=2.0, fare=8.0, total=10.0))

    return pd.DataFrame(rows)
