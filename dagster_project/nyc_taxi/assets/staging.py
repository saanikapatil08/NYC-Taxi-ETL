"""Staging assets: stream-clean parquet partitions into Postgres."""

from dagster import (
    AssetExecutionContext,
    Backoff,
    Jitter,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_project.nyc_taxi.partitions import (
    monthly_partitions,
    partition_year_month,
)
from shared.extract import local_target_path
from shared.load import stream_clean_load_staging

retry_policy = RetryPolicy(
    max_retries=2,
    delay=15,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)


@asset(
    partitions_def=monthly_partitions,
    group_name="staging",
    deps=["raw_trip_file"],
    retry_policy=retry_policy,
    description=(
        "Stream raw parquet through validate/clean and replace staging.fct_trips "
        "for the partition (bounded memory)."
    ),
)
def staging_trips_loaded(context: AssetExecutionContext) -> int:
    year, month = partition_year_month(context.partition_key)
    path = str(local_target_path(year, month))
    rows = stream_clean_load_staging(path, year=year, month=month)
    context.add_output_metadata(
        {
            "rows_loaded": MetadataValue.int(rows),
            "warehouse_table": MetadataValue.text("staging.fct_trips"),
            "partition": MetadataValue.text(f"{year:04d}-{month:02d}"),
        }
    )
    return rows
