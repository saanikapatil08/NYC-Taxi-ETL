"""Transformation logic that turns raw TLC trip data into a clean staging frame.

The transform is deliberately pure (DataFrame in, DataFrame out) so it can be
unit-tested without touching the network or the warehouse.
"""

from __future__ import annotations

import pandas as pd

from shared.logging_config import get_logger

log = get_logger(__name__)


# Sanity bounds applied during cleaning
MAX_TRIP_HOURS = 6
MIN_TRIP_DISTANCE = 0.0
MAX_TRIP_DISTANCE = 200.0
MIN_FARE = 0.0
MAX_FARE = 500.0


def clean_trips(raw: pd.DataFrame, *, year: int, month: int) -> pd.DataFrame:
    """Normalize column names and apply business cleaning rules.

    Drops rows that are clearly invalid (zero distance, negative fare, trips
    that span more than ``MAX_TRIP_HOURS``, trips that fall outside the target
    year/month partition).
    """
    if raw.empty:
        log.warning("clean_trips received an empty dataframe")
        return raw.copy()

    df = raw.rename(
        columns={
            "VendorID": "vendor_id",
            "tpep_pickup_datetime": "pickup_ts",
            "tpep_dropoff_datetime": "dropoff_ts",
            "PULocationID": "pickup_zone_id",
            "DOLocationID": "dropoff_zone_id",
        }
    ).copy()

    # Coerce dtypes deterministically
    df["pickup_ts"] = pd.to_datetime(df["pickup_ts"], errors="coerce")
    df["dropoff_ts"] = pd.to_datetime(df["dropoff_ts"], errors="coerce")
    df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce")
    df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
    df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
    df["tip_amount"] = pd.to_numeric(df.get("tip_amount", 0), errors="coerce")
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    df["payment_type"] = pd.to_numeric(df["payment_type"], errors="coerce")

    # Compute derived fields
    df["trip_duration_min"] = (df["dropoff_ts"] - df["pickup_ts"]).dt.total_seconds() / 60.0
    df["trip_distance_mi"] = df["trip_distance"]
    df["trip_year"] = df["pickup_ts"].dt.year
    df["trip_month"] = df["pickup_ts"].dt.month

    # Drop the must-have nulls
    df = df.dropna(
        subset=[
            "pickup_ts",
            "dropoff_ts",
            "pickup_zone_id",
            "dropoff_zone_id",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "passenger_count",
            "payment_type",
            "vendor_id",
        ]
    )

    # Apply business rules
    mask = (
        (df["trip_duration_min"] > 0)
        & (df["trip_duration_min"] <= MAX_TRIP_HOURS * 60)
        & (df["trip_distance_mi"] > MIN_TRIP_DISTANCE)
        & (df["trip_distance_mi"] <= MAX_TRIP_DISTANCE)
        & (df["fare_amount"] >= MIN_FARE)
        & (df["fare_amount"] <= MAX_FARE)
        & (df["total_amount"] >= 0)
        & (df["passenger_count"] >= 1)
        & (df["trip_year"] == year)
        & (df["trip_month"] == month)
    )
    cleaned = df.loc[mask].copy()
    cleaned["tip_amount"] = cleaned["tip_amount"].fillna(0).clip(lower=0)

    # Final dtype tightening so downstream loads are predictable
    cleaned["vendor_id"] = cleaned["vendor_id"].astype(int)
    cleaned["passenger_count"] = cleaned["passenger_count"].astype(int)
    cleaned["pickup_zone_id"] = cleaned["pickup_zone_id"].astype(int)
    cleaned["dropoff_zone_id"] = cleaned["dropoff_zone_id"].astype(int)
    cleaned["payment_type"] = cleaned["payment_type"].astype(int)
    cleaned["trip_year"] = cleaned["trip_year"].astype(int)
    cleaned["trip_month"] = cleaned["trip_month"].astype(int)

    out_cols = [
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
    ]
    cleaned = cleaned[out_cols]

    dropped = len(df) - len(cleaned)
    log.info(
        "clean_trips: kept %d rows, dropped %d rows (%.1f%%) for %04d-%02d",
        len(cleaned),
        dropped,
        100 * dropped / max(len(df), 1),
        year,
        month,
    )
    return cleaned
