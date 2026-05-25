"""Smoke test that the Dagster definitions module imports and loads cleanly."""

from __future__ import annotations


def test_definitions_import() -> None:
    from dagster_project.nyc_taxi.definitions import defs

    keys = {str(k) for k in defs.get_asset_graph().all_asset_keys}
    assert "AssetKey(['raw_trip_file'])" in keys
    assert "AssetKey(['staging_trips_loaded'])" in keys
    assert "AssetKey(['fct_daily_trips_refreshed'])" in keys
    assert "AssetKey(['row_count_anomaly_recorded'])" in keys

    job_names = [j.name for j in defs.get_all_job_defs()]
    assert "nyc_taxi_etl_job" in job_names

    schedule_names = [s.name for s in defs.schedules]
    assert "nyc_taxi_etl_monthly" in schedule_names

    sensor_names = [s.name for s in defs.sensors]
    assert "nyc_taxi_failure_alert" in sensor_names
    assert "nyc_taxi_run_health" in sensor_names
