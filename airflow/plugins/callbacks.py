"""Airflow callback functions: failure alerts, retry alerts, SLA misses."""

from __future__ import annotations

from typing import Any

from shared.alerts import AlertPayload, send_alert


def _ctx_to_payload(context: dict[str, Any], severity: str, title: str) -> AlertPayload:
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    return AlertPayload(
        title=title,
        severity=severity,
        pipeline="airflow:nyc_taxi_etl",
        component=f"{getattr(ti, 'dag_id', 'unknown')}.{getattr(ti, 'task_id', 'unknown')}",
        message=str(context.get("exception") or "see task logs"),
        context={
            "execution_date": str(context.get("logical_date") or context.get("execution_date")),
            "run_id": getattr(dag_run, "run_id", "unknown"),
            "try_number": getattr(ti, "try_number", "unknown"),
            "log_url": getattr(ti, "log_url", "unknown"),
        },
    )


def task_failure_alert(context: dict[str, Any]) -> None:
    send_alert(_ctx_to_payload(context, "error", "Airflow task failed"))


def task_retry_alert(context: dict[str, Any]) -> None:
    send_alert(_ctx_to_payload(context, "warning", "Airflow task is retrying"))


def sla_miss_alert(
    dag,  # type: ignore[no-untyped-def]
    task_list,  # type: ignore[no-untyped-def]
    blocking_task_list,  # type: ignore[no-untyped-def]
    slas,  # type: ignore[no-untyped-def]
    blocking_tis,  # type: ignore[no-untyped-def]
) -> None:
    send_alert(
        AlertPayload(
            title="Airflow SLA missed",
            severity="warning",
            pipeline="airflow:nyc_taxi_etl",
            component=getattr(dag, "dag_id", "unknown"),
            message=f"SLA missed for {task_list}",
            context={
                "blocking_tasks": str(blocking_task_list),
                "slas": str(slas),
            },
        )
    )
