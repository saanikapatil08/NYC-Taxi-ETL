"""Jobs and schedules for the Dagster pipeline."""

from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

# Whole-pipeline job: materialize every NYC Taxi asset for one partition.
# Partition definition is inferred from the selected assets, so we don't pass
# `partitions_def` explicitly (deprecated in Dagster 1.8+).
nyc_taxi_etl_job = define_asset_job(
    name="nyc_taxi_etl_job",
    selection=AssetSelection.all(),
    description="Run the full NYC Taxi ETL for the selected monthly partition.",
)


# 06:00 UTC on the 5th of every month, mirroring the Airflow DAG.
nyc_taxi_etl_schedule = ScheduleDefinition(
    job=nyc_taxi_etl_job,
    cron_schedule="0 6 5 * *",
    name="nyc_taxi_etl_monthly",
    description="Monthly schedule for the NYC Taxi ETL job.",
    execution_timezone="UTC",
)
