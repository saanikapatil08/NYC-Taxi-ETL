"""Top-level Dagster Definitions object loaded by the workspace."""

from __future__ import annotations

from dagster import Definitions, in_process_executor

from dagster_project.nyc_taxi.assets import (
    dim_zone_refreshed,
    fct_daily_trips_refreshed,
    raw_trip_file,
    row_count_anomaly_recorded,
    staging_trips_loaded,
)
from dagster_project.nyc_taxi.checks import staging_quality_check
from dagster_project.nyc_taxi.jobs import (
    nyc_taxi_etl_job,
    nyc_taxi_etl_schedule,
)
from dagster_project.nyc_taxi.resources import (
    AlertResource,
    SettingsResource,
    WarehouseResource,
)
from dagster_project.nyc_taxi.sensors import (
    nyc_taxi_failure_alert,
    nyc_taxi_run_health,
)

defs = Definitions(
    # Single-process runs avoid OOM on large monthly Parquets under Docker defaults.
    executor=in_process_executor,
    assets=[
        raw_trip_file,
        staging_trips_loaded,
        row_count_anomaly_recorded,
        dim_zone_refreshed,
        fct_daily_trips_refreshed,
    ],
    asset_checks=[
        staging_quality_check,
    ],
    jobs=[nyc_taxi_etl_job],
    schedules=[nyc_taxi_etl_schedule],
    sensors=[nyc_taxi_failure_alert, nyc_taxi_run_health],
    resources={
        "warehouse": WarehouseResource(),
        "alerts": AlertResource(),
        "settings": SettingsResource(),
    },
)
