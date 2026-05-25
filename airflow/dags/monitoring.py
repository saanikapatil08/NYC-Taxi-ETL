"""Daily monitoring DAG that re-runs the row-count anomaly detector across the
last six partitions and pages oncall on any drift. Runs even when the main ETL
is not scheduled to fire so silent failures get caught."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from plugins.callbacks import task_failure_alert, task_retry_alert  # noqa: E402
from shared.alerts import AlertPayload, send_alert  # noqa: E402
from shared.anomaly import detect_row_count_anomaly  # noqa: E402
from shared.logging_config import get_logger  # noqa: E402

log = get_logger("airflow.monitoring")


def task_scan_recent_partitions(**kwargs) -> None:
    today: datetime = kwargs["logical_date"]
    anomalies = []
    cursor = today.replace(day=1)
    for _ in range(6):
        cursor -= timedelta(days=1)
        cursor = cursor.replace(day=1)
        result = detect_row_count_anomaly(cursor.year, cursor.month)
        log.info("scan: %s", result)
        if result.is_anomaly:
            anomalies.append(result)

    if anomalies:
        send_alert(
            AlertPayload(
                title="Row-count anomalies detected during scheduled scan",
                severity="error",
                pipeline="airflow:nyc_taxi_monitoring",
                component="scan_recent_partitions",
                message=f"{len(anomalies)} anomalous partition(s)",
                context={f"{a.year}-{a.month:02d}": a.reason for a in anomalies},
            )
        )


with DAG(
    dag_id="nyc_taxi_monitoring",
    description="Daily anomaly scan and alerting for the NYC Taxi pipeline.",
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": task_failure_alert,
        "on_retry_callback": task_retry_alert,
    },
    start_date=datetime(2024, 1, 1),
    schedule="0 7 * * *",  # daily at 07:00 UTC
    catchup=False,
    tags=["nyc-taxi", "monitoring"],
):
    PythonOperator(
        task_id="scan_recent_partitions",
        python_callable=task_scan_recent_partitions,
    )
