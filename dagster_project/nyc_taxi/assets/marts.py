"""Marts assets: refresh dim_zone and fct_daily_trips from staging."""

from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, asset

from dagster_project.nyc_taxi.partitions import (
    monthly_partitions,
    partition_year_month,
)
from shared.load import execute_transformation_sql

PROJECT_ROOT = Path(__file__).resolve().parents[3]


@asset(
    partitions_def=monthly_partitions,
    group_name="marts",
    deps=["staging_trips_loaded"],
    description="Upsert observed pickup/dropoff zones into marts.dim_zone.",
)
def dim_zone_refreshed(context: AssetExecutionContext) -> str:
    year, month = partition_year_month(context.partition_key)
    execute_transformation_sql(
        PROJECT_ROOT / "sql" / "transformations" / "dim_zone.sql",
        params={"year": year, "month": month},
    )
    context.add_output_metadata({"partition": f"{year:04d}-{month:02d}"})
    return "ok"


@asset(
    partitions_def=monthly_partitions,
    group_name="marts",
    deps=["staging_trips_loaded", "dim_zone_refreshed"],
    description="Refresh marts.fct_daily_trips for the partition.",
)
def fct_daily_trips_refreshed(context: AssetExecutionContext) -> str:
    year, month = partition_year_month(context.partition_key)
    execute_transformation_sql(
        PROJECT_ROOT / "sql" / "transformations" / "fct_daily_trips.sql",
        params={"year": year, "month": month},
    )
    context.add_output_metadata(
        {
            "partition": f"{year:04d}-{month:02d}",
            "table": MetadataValue.text("marts.fct_daily_trips"),
        }
    )
    return "ok"
