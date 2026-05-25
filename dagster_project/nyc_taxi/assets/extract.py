"""Extract assets: download monthly parquet and validate raw schema."""

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
from shared.extract import download_month
from shared.load import validate_raw_parquet_file

retry_policy = RetryPolicy(
    max_retries=3,
    delay=10,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)


@asset(
    partitions_def=monthly_partitions,
    group_name="extract",
    retry_policy=retry_policy,
    description="Download the monthly NYC TLC parquet for the partition.",
)
def raw_trip_file(context: AssetExecutionContext) -> dict:
    year, month = partition_year_month(context.partition_key)
    result = download_month(year, month)

    nrows = validate_raw_parquet_file(result.local_path)

    context.add_output_metadata(
        {
            "year": year,
            "month": month,
            "source_url": MetadataValue.url(result.source_url),
            "local_path": MetadataValue.path(result.local_path),
            "rows": MetadataValue.int(nrows),
            "bytes": MetadataValue.int(result.bytes_written),
        }
    )
    return {
        "year": year,
        "month": month,
        "local_path": result.local_path,
        "source_url": result.source_url,
        "rows": nrows,
    }
