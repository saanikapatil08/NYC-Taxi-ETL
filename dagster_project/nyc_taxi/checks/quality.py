"""Asset checks that wrap the shared quality suites.

In Dagster, asset checks are first-class citizens: they run after their target
asset materializes and surface in the UI as pass/fail next to the asset.
"""

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

from dagster_project.nyc_taxi.partitions import partition_year_month
from shared.config import get_settings
from shared.quality_checks import (
    QualitySuiteResult,
    run_dataframe_suite_on_staging_partition,
    run_warehouse_suite,
)


def _suite_to_result(suite: QualitySuiteResult, *, asset_key: str) -> AssetCheckResult:
    return AssetCheckResult(
        passed=suite.passed,
        severity=AssetCheckSeverity.ERROR if suite.failed else AssetCheckSeverity.WARN,
        metadata={
            "checks_run": len(suite.results),
            "errors": len(suite.failed),
            "warnings": len(suite.warnings),
            "failed_checks": ", ".join(r.name for r in suite.failed) or "none",
            "warn_checks": ", ".join(r.name for r in suite.warnings) or "none",
        },
        description=f"{asset_key} quality suite",
    )


@asset_check(
    asset="staging_trips_loaded",
    description="Logical dataframe gates on staging + warehouse row/uniqueness checks.",
)
def staging_quality_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
    settings = get_settings()
    year, month = partition_year_month(context.op_execution_context.partition_key)
    df_suite = run_dataframe_suite_on_staging_partition(year, month)
    wh_suite = run_warehouse_suite(year, month, settings.min_expected_rows)
    suite = QualitySuiteResult(results=df_suite.results + wh_suite.results)
    return _suite_to_result(suite, asset_key="staging_trips_loaded")
