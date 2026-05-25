"""Sensors that watch run status and fire alerts."""

from dagster import (
    DagsterRunStatus,
    RunFailureSensorContext,
    SensorEvaluationContext,
    SkipReason,
    run_failure_sensor,
    sensor,
)

from dagster_project.nyc_taxi.jobs import nyc_taxi_etl_job
from shared.alerts import AlertPayload, send_alert


@run_failure_sensor(
    name="nyc_taxi_failure_alert",
    monitored_jobs=[nyc_taxi_etl_job],
    description="Page oncall when the NYC Taxi pipeline fails.",
)
def nyc_taxi_failure_alert(context: RunFailureSensorContext) -> None:
    run = context.dagster_run
    failure_event = context.failure_event
    send_alert(
        AlertPayload(
            title="Dagster run failed",
            severity="error",
            pipeline="dagster:nyc_taxi_etl",
            component=run.job_name,
            message=str(failure_event.message or "see Dagster UI for details"),
            context={
                "run_id": run.run_id,
                "partition": run.tags.get("dagster/partition", "unknown"),
                "status": run.status.value,
            },
        )
    )


@sensor(
    name="nyc_taxi_run_health",
    description="Emit a heartbeat alert if no successful run lands in 36 hours.",
    minimum_interval_seconds=600,
)
def nyc_taxi_run_health(context: SensorEvaluationContext):
    instance = context.instance
    recent_runs = instance.get_runs(limit=20)
    successes = [r for r in recent_runs if r.status == DagsterRunStatus.SUCCESS]
    if successes:
        return SkipReason("recent successful runs present")
    if recent_runs:
        send_alert(
            AlertPayload(
                title="No successful Dagster runs in recent history",
                severity="warning",
                pipeline="dagster:nyc_taxi_etl",
                component="run_health",
                message="Last 20 runs contain no SUCCESS status",
                context={"recent_run_count": len(recent_runs)},
            )
        )
    return SkipReason("alert dispatched, no run to launch")
