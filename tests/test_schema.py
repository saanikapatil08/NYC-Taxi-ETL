"""Schema validation tests (pandera)."""

from __future__ import annotations

import pandas as pd

from shared.schema import validate_raw, validate_staging
from shared.transform import clean_trips


def test_validate_raw_accepts_real_shape(raw_frame: pd.DataFrame) -> None:
    validated = validate_raw(raw_frame)
    assert len(validated) == len(raw_frame)


def test_validate_raw_accepts_negative_tip_sources(raw_frame: pd.DataFrame) -> None:
    bad = raw_frame.copy()
    bad.loc[0, "tip_amount"] = -1.0
    validate_raw(bad)


def test_negative_tip_clipped_before_staging(raw_frame: pd.DataFrame) -> None:
    bad = raw_frame.copy()
    bad.loc[0, "tip_amount"] = -5.0
    cleaned = clean_trips(bad, year=2024, month=3)
    validate_staging(cleaned)
    assert (cleaned["tip_amount"] >= 0).all()


def test_validate_staging_passes_after_clean(raw_frame: pd.DataFrame) -> None:
    cleaned = clean_trips(raw_frame, year=2024, month=3)
    validate_staging(cleaned)
    assert {"vendor_id", "pickup_zone_id", "trip_year", "trip_month"}.issubset(cleaned.columns)
