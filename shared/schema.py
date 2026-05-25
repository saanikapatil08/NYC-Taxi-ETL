"""Schema definitions enforced at the boundary of every pipeline stage.

Pandera gives us strict, declarative dataframe contracts that fail fast when
the upstream feed shifts. Both Airflow and Dagster invoke the same validators.
"""

from __future__ import annotations

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

# Yellow taxi raw schema as published by NYC TLC
RAW_TRIP_SCHEMA: DataFrameSchema = DataFrameSchema(
    columns={
        "VendorID": Column(pa.Int, nullable=True),
        "tpep_pickup_datetime": Column(pa.DateTime, nullable=False),
        "tpep_dropoff_datetime": Column(pa.DateTime, nullable=False),
        "passenger_count": Column(pa.Float, nullable=True, checks=Check.ge(0)),
        "trip_distance": Column(pa.Float, nullable=True, checks=Check.ge(0)),
        "RatecodeID": Column(pa.Float, nullable=True),
        "store_and_fwd_flag": Column(pa.String, nullable=True),
        "PULocationID": Column(pa.Int, nullable=False),
        "DOLocationID": Column(pa.Int, nullable=False),
        "payment_type": Column(pa.Int, nullable=True),
        "fare_amount": Column(pa.Float, nullable=True),
        "extra": Column(pa.Float, nullable=True),
        "mta_tax": Column(pa.Float, nullable=True),
        # TLC publishes occasional negatives/refunds on amounts; clip at clean time.
        "tip_amount": Column(pa.Float, nullable=True),
        "tolls_amount": Column(pa.Float, nullable=True),
        "improvement_surcharge": Column(pa.Float, nullable=True),
        "total_amount": Column(pa.Float, nullable=True),
        "congestion_surcharge": Column(pa.Float, nullable=True, required=False),
        "airport_fee": Column(pa.Float, nullable=True, required=False),
    },
    strict=False,  # ignore extra columns the TLC sometimes adds mid-year
    coerce=True,
)


# Cleaned staging schema produced by ``shared.transform.clean_trips``
STAGING_TRIP_SCHEMA: DataFrameSchema = DataFrameSchema(
    columns={
        "vendor_id": Column(pa.Int, nullable=False),
        "pickup_ts": Column(pa.DateTime, nullable=False),
        "dropoff_ts": Column(pa.DateTime, nullable=False),
        "passenger_count": Column(pa.Int, nullable=False, checks=Check.ge(1)),
        "trip_distance_mi": Column(pa.Float, nullable=False, checks=Check.gt(0)),
        "trip_duration_min": Column(pa.Float, nullable=False, checks=Check.gt(0)),
        "pickup_zone_id": Column(pa.Int, nullable=False),
        "dropoff_zone_id": Column(pa.Int, nullable=False),
        "payment_type": Column(pa.Int, nullable=False),
        "fare_amount": Column(pa.Float, nullable=False, checks=Check.ge(0)),
        "tip_amount": Column(pa.Float, nullable=False, checks=Check.ge(0)),
        "total_amount": Column(pa.Float, nullable=False, checks=Check.ge(0)),
        "trip_year": Column(pa.Int, nullable=False),
        "trip_month": Column(pa.Int, nullable=False, checks=Check.in_range(1, 12)),
    },
    strict=True,
    coerce=True,
)


def validate_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Validate raw input. Raises ``pandera.errors.SchemaError`` on violation."""
    return RAW_TRIP_SCHEMA.validate(df, lazy=True)


def validate_staging(df: pd.DataFrame) -> pd.DataFrame:
    """Validate the cleaned staging dataframe."""
    return STAGING_TRIP_SCHEMA.validate(df, lazy=True)
