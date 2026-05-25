"""Ops assets: run the row-count anomaly detector and persist the snapshot."""

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
)
from sqlalchemy import text

from dagster_project.nyc_taxi.partitions import (
    monthly_partitions,
    partition_year_month,
)
from dagster_project.nyc_taxi.resources import WarehouseResource
from shared.anomaly import (
    RowCountAnomaly,
    assert_no_anomaly,
    detect_row_count_anomaly,
)

PIPELINE_NAME = "dagster:nyc_taxi_etl"


@asset(
    partitions_def=monthly_partitions,
    group_name="ops",
    deps=["staging_trips_loaded"],
    description=(
        "Compare the partition's row count against the trailing median and raise "
        "if deviation exceeds the configured threshold."
    ),
)
def row_count_anomaly_recorded(
    context: AssetExecutionContext,
    warehouse: WarehouseResource,
) -> dict:
    year, month = partition_year_month(context.partition_key)
    result = detect_row_count_anomaly(year, month)

    engine = warehouse.get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ops.row_count_history (
                    pipeline, trip_year, trip_month, row_count,
                    historical_median, deviation_pct, is_anomaly, reason
                ) VALUES (
                    :pipeline, :year, :month, :row_count,
                    :historical_median, :deviation_pct, :is_anomaly, :reason
                )
                """
            ),
            {
                "pipeline": PIPELINE_NAME,
                "year": year,
                "month": month,
                "row_count": result.current_rows,
                "historical_median": result.historical_median,
                "deviation_pct": result.deviation_pct,
                "is_anomaly": result.is_anomaly,
                "reason": result.reason,
            },
        )

    context.add_output_metadata(
        {
            "current_rows": MetadataValue.int(result.current_rows),
            "historical_median": MetadataValue.float(result.historical_median or 0.0),
            "deviation_pct": MetadataValue.float(result.deviation_pct or 0.0),
            "is_anomaly": MetadataValue.bool(result.is_anomaly),
            "reason": MetadataValue.text(result.reason),
        }
    )

    try:
        assert_no_anomaly(result)
    except RowCountAnomaly:
        # Re-raise so the asset materialization fails and the failure sensor pages.
        raise
    return {
        "year": year,
        "month": month,
        "current_rows": result.current_rows,
        "is_anomaly": result.is_anomaly,
    }
