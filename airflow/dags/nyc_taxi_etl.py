"""Production NYC Taxi ETL DAG.

Stages, in order, with hard dependencies:

    init_warehouse  -> extract  -> validate_raw_schema  -> load_staging
                                                          |
                                                          +-> validate_staging_schema
                                                          +-> dataframe_quality_gate
                                                          +-> warehouse_quality_gate
                                                          +-> row_count_anomaly_gate
                                                          +-> transform_marts
                                                          +-> record_metrics

Every task has retries, exponential backoff, structured logging, an
``on_failure_callback`` that pages oncall via the shared alert sender, and an
``on_retry_callback`` that emits a warning so flaky tasks become visible
before they break.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Make the project root importable when Airflow loads DAGs from /opt/airflow/dags
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from plugins.callbacks import (  # noqa: E402
    sla_miss_alert,
    task_failure_alert,
    task_retry_alert,
)
from shared.anomaly import (  # noqa: E402
    assert_no_anomaly,
    detect_row_count_anomaly,
)
from shared.config import get_settings  # noqa: E402
from shared.db import get_engine  # noqa: E402
from shared.extract import download_month  # noqa: E402
from shared.load import (  # noqa: E402
    execute_transformation_sql,
    init_warehouse,
    stream_clean_load_staging,
    validate_raw_parquet_file,
)
from shared.logging_config import get_logger  # noqa: E402
from shared.quality_checks import (  # noqa: E402
    assert_suite_passed,
    run_dataframe_suite_on_staging_partition,
    run_warehouse_suite,
)
from sqlalchemy import text  # noqa: E402

log = get_logger("airflow.nyc_taxi_etl")

PIPELINE_NAME = "airflow:nyc_taxi_etl"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": task_failure_alert,
    "on_retry_callback": task_retry_alert,
    "sla": timedelta(hours=2),
}


def _partition_from_context(**kwargs) -> tuple[int, int]:
    """Resolve the (year, month) partition from execution context or DAG params."""
    params = kwargs.get("params") or {}
    if params.get("year") and params.get("month"):
        return int(params["year"]), int(params["month"])
    logical_date: datetime = kwargs["logical_date"]
    # Pipeline runs against the previous month (data is published monthly).
    target = (logical_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    return target.year, target.month


def _run_id(year: int, month: int, **kwargs) -> str:
    return f"{PIPELINE_NAME}:{year:04d}-{month:02d}:{kwargs['ts_nodash']}"


def task_init_warehouse(**_) -> None:
    init_warehouse()


def task_extract(**kwargs) -> dict:
    year, month = _partition_from_context(**kwargs)
    result = download_month(year, month)
    return {
        "year": result.year,
        "month": result.month,
        "local_path": result.local_path,
        "source_url": result.source_url,
        "bytes_written": result.bytes_written,
    }


def task_validate_raw(**kwargs) -> int:
    ti = kwargs["ti"]
    extract_result = ti.xcom_pull(task_ids="extract.download_month")
    nrows = validate_raw_parquet_file(extract_result["local_path"])
    log.info("Raw schema validation passed for %d rows", nrows)
    return nrows


def task_load_staging(**kwargs) -> int:
    ti = kwargs["ti"]
    extract_result = ti.xcom_pull(task_ids="extract.download_month")
    year = extract_result["year"]
    month = extract_result["month"]
    return stream_clean_load_staging(
        extract_result["local_path"], year=year, month=month
    )


def task_dataframe_quality(**kwargs) -> None:
    ti = kwargs["ti"]
    extract_result = ti.xcom_pull(task_ids="extract.download_month")
    suite = run_dataframe_suite_on_staging_partition(
        extract_result["year"], extract_result["month"]
    )
    assert_suite_passed(suite, label="dataframe")


def task_warehouse_quality(**kwargs) -> None:
    settings = get_settings()
    ti = kwargs["ti"]
    extract_result = ti.xcom_pull(task_ids="extract.download_month")
    suite = run_warehouse_suite(
        extract_result["year"], extract_result["month"], settings.min_expected_rows
    )
    assert_suite_passed(suite, label="warehouse")


def task_anomaly_gate(**kwargs) -> dict:
    ti = kwargs["ti"]
    extract_result = ti.xcom_pull(task_ids="extract.download_month")
    year = extract_result["year"]
    month = extract_result["month"]
    result = detect_row_count_anomaly(year, month)
    log.info("Anomaly result: %s", result)

    engine = get_engine()
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
    assert_no_anomaly(result)
    return {"year": year, "month": month, "rows": result.current_rows}


def task_transform_marts(**kwargs) -> None:
    ti = kwargs["ti"]
    extract_result = ti.xcom_pull(task_ids="extract.download_month")
    year = extract_result["year"]
    month = extract_result["month"]
    execute_transformation_sql(
        PROJECT_ROOT / "sql" / "transformations" / "dim_zone.sql",
        params={"year": year, "month": month},
    )
    execute_transformation_sql(
        PROJECT_ROOT / "sql" / "transformations" / "fct_daily_trips.sql",
        params={"year": year, "month": month},
    )


def task_record_run(status: str, **kwargs) -> None:
    ti = kwargs["ti"]
    extract_result = ti.xcom_pull(task_ids="extract.download_month") or {}
    year = extract_result.get("year") or kwargs["logical_date"].year
    month = extract_result.get("month") or kwargs["logical_date"].month
    rows = ti.xcom_pull(task_ids="load.load_staging")

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ops.pipeline_runs (
                    pipeline, run_key, trip_year, trip_month,
                    finished_at, status, rows_loaded
                ) VALUES (
                    :pipeline, :run_key, :year, :month,
                    NOW(), :status, :rows_loaded
                )
                """
            ),
            {
                "pipeline": PIPELINE_NAME,
                "run_key": _run_id(year, month, **kwargs),
                "year": year,
                "month": month,
                "status": status,
                "rows_loaded": rows,
            },
        )


with DAG(
    dag_id="nyc_taxi_etl",
    description="NYC Taxi monthly ETL with schema validation, quality gates, and anomaly detection.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 5 * *",  # 06:00 UTC on the 5th of every month
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_alert,
    tags=["nyc-taxi", "etl", "production"],
    params={"year": None, "month": None},
) as dag:

    init = PythonOperator(
        task_id="init_warehouse",
        python_callable=task_init_warehouse,
    )

    with TaskGroup(group_id="extract") as extract_group:
        extract = PythonOperator(
            task_id="download_month",
            python_callable=task_extract,
        )
        validate_raw_schema = PythonOperator(
            task_id="validate_raw_schema",
            python_callable=task_validate_raw,
        )
        extract >> validate_raw_schema

    with TaskGroup(group_id="load") as load_group:
        load = PythonOperator(
            task_id="load_staging",
            python_callable=task_load_staging,
        )

    with TaskGroup(group_id="quality") as quality_group:
        df_quality = PythonOperator(
            task_id="dataframe_quality_gate",
            python_callable=task_dataframe_quality,
        )
        warehouse_quality = PythonOperator(
            task_id="warehouse_quality_gate",
            python_callable=task_warehouse_quality,
        )
        anomaly_gate = PythonOperator(
            task_id="row_count_anomaly_gate",
            python_callable=task_anomaly_gate,
        )
        df_quality >> warehouse_quality >> anomaly_gate

    transform = PythonOperator(
        task_id="transform_marts",
        python_callable=task_transform_marts,
    )

    record = PythonOperator(
        task_id="record_pipeline_run",
        python_callable=task_record_run,
        op_kwargs={"status": "succeeded"},
        trigger_rule="all_success",
    )

    init >> extract_group >> load_group >> quality_group >> transform >> record
