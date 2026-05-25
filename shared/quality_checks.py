"""Multi-layer data-quality gates run after every load.

Each check returns a structured ``CheckResult``. The ``run_quality_suite``
helper aggregates results and raises ``QualityCheckFailure`` if any gate is
breached, which both the Airflow task and the Dagster asset check pick up.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pandas as pd
from sqlalchemy import text

from shared.db import get_engine
from shared.logging_config import get_logger

log = get_logger(__name__)


class QualityCheckFailure(RuntimeError):
    """Raised when any quality gate fails."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: str  # "error" or "warn"
    metric: float | int | None = None
    threshold: float | int | None = None
    detail: str = ""


@dataclass
class QualitySuiteResult:
    results: list[CheckResult] = field(default_factory=list)

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == "error"]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == "warn"]

    @property
    def passed(self) -> bool:
        return not self.failed


# ---------------------------------------------------------------------------
# Dataframe-level checks (run on the in-memory cleaned frame)
# ---------------------------------------------------------------------------


def check_no_nulls_required(df: pd.DataFrame) -> CheckResult:
    cols = [
        "vendor_id",
        "pickup_ts",
        "dropoff_ts",
        "pickup_zone_id",
        "dropoff_zone_id",
        "fare_amount",
        "total_amount",
    ]
    bad = int(df[cols].isnull().any(axis=1).sum())
    return CheckResult(
        name="no_nulls_in_required_columns",
        passed=bad == 0,
        severity="error",
        metric=bad,
        threshold=0,
        detail=f"{bad} rows had nulls in required columns",
    )


def check_pickup_before_dropoff(df: pd.DataFrame) -> CheckResult:
    bad = int((df["dropoff_ts"] < df["pickup_ts"]).sum())
    return CheckResult(
        name="pickup_before_dropoff",
        passed=bad == 0,
        severity="error",
        metric=bad,
        threshold=0,
        detail=f"{bad} trips had dropoff before pickup",
    )


def check_positive_fare(df: pd.DataFrame) -> CheckResult:
    bad = int((df["fare_amount"] < 0).sum())
    return CheckResult(
        name="positive_fare_amount",
        passed=bad == 0,
        severity="error",
        metric=bad,
        threshold=0,
        detail=f"{bad} trips had negative fare",
    )


def check_distinct_zone_count(df: pd.DataFrame, min_zones: int = 50) -> CheckResult:
    distinct = int(df["pickup_zone_id"].nunique())
    return CheckResult(
        name="minimum_distinct_pickup_zones",
        passed=distinct >= min_zones,
        severity="warn",
        metric=distinct,
        threshold=min_zones,
        detail=f"only {distinct} distinct pickup zones present",
    )


# ---------------------------------------------------------------------------
# Warehouse-level checks (run after data is in Postgres)
# ---------------------------------------------------------------------------


def check_warehouse_row_count(year: int, month: int, expected_min: int) -> CheckResult:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM staging.fct_trips
                WHERE trip_year = :year AND trip_month = :month
                """
            ),
            {"year": year, "month": month},
        )
        count = int(result.scalar() or 0)
    return CheckResult(
        name="warehouse_row_count_above_minimum",
        passed=count >= expected_min,
        severity="error",
        metric=count,
        threshold=expected_min,
        detail=f"{count} rows landed for {year:04d}-{month:02d}",
    )


def check_warehouse_unique_trip_id(year: int, month: int) -> CheckResult:
    engine = get_engine()
    with engine.connect() as conn:
        dupes = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                      SELECT trip_id, COUNT(*) AS c
                      FROM staging.fct_trips
                      WHERE trip_year = :year AND trip_month = :month
                      GROUP BY trip_id
                      HAVING COUNT(*) > 1
                    ) t
                    """
                ),
                {"year": year, "month": month},
            ).scalar()
            or 0
        )
    return CheckResult(
        name="warehouse_unique_trip_id",
        passed=dupes == 0,
        severity="error",
        metric=dupes,
        threshold=0,
        detail=f"{dupes} duplicate trip_id values",
    )


# ---------------------------------------------------------------------------
# Suite runner
# ---------------------------------------------------------------------------


def run_dataframe_suite(df: pd.DataFrame) -> QualitySuiteResult:
    runners: list[Callable[[pd.DataFrame], CheckResult]] = [
        check_no_nulls_required,
        check_pickup_before_dropoff,
        check_positive_fare,
        check_distinct_zone_count,
    ]
    suite = QualitySuiteResult(results=[fn(df) for fn in runners])
    _log_suite("dataframe", suite)
    return suite


def _partition_filter() -> str:
    return "trip_year = :year AND trip_month = :month"


def check_staging_nulls_partition(year: int, month: int) -> CheckResult:
    engine = get_engine()
    with engine.connect() as conn:
        bad = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM staging.fct_trips
                    WHERE {_partition_filter()}
                      AND (
                        vendor_id IS NULL OR pickup_ts IS NULL OR dropoff_ts IS NULL
                        OR pickup_zone_id IS NULL OR dropoff_zone_id IS NULL
                        OR fare_amount IS NULL OR total_amount IS NULL
                      )
                    """
                ),
                {"year": year, "month": month},
            ).scalar()
            or 0
        )
    return CheckResult(
        name="no_nulls_in_required_columns",
        passed=bad == 0,
        severity="error",
        metric=bad,
        threshold=0,
        detail=f"{bad} rows had nulls in required columns",
    )


def check_staging_temporal_order_partition(year: int, month: int) -> CheckResult:
    engine = get_engine()
    with engine.connect() as conn:
        bad = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM staging.fct_trips
                    WHERE {_partition_filter()} AND dropoff_ts < pickup_ts
                    """
                ),
                {"year": year, "month": month},
            ).scalar()
            or 0
        )
    return CheckResult(
        name="pickup_before_dropoff",
        passed=bad == 0,
        severity="error",
        metric=bad,
        threshold=0,
        detail=f"{bad} trips had dropoff before pickup",
    )


def check_staging_non_negative_fare_partition(year: int, month: int) -> CheckResult:
    engine = get_engine()
    with engine.connect() as conn:
        bad = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM staging.fct_trips
                    WHERE {_partition_filter()} AND fare_amount < 0
                    """
                ),
                {"year": year, "month": month},
            ).scalar()
            or 0
        )
    return CheckResult(
        name="positive_fare_amount",
        passed=bad == 0,
        severity="error",
        metric=bad,
        threshold=0,
        detail=f"{bad} trips had negative fare",
    )


def check_staging_distinct_zones_partition(year: int, month: int, min_zones: int = 50) -> CheckResult:
    engine = get_engine()
    with engine.connect() as conn:
        distinct = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(DISTINCT pickup_zone_id) FROM staging.fct_trips
                    WHERE {_partition_filter()}
                    """
                ),
                {"year": year, "month": month},
            ).scalar()
            or 0
        )
    return CheckResult(
        name="minimum_distinct_pickup_zones",
        passed=distinct >= min_zones,
        severity="warn",
        metric=distinct,
        threshold=min_zones,
        detail=f"only {distinct} distinct pickup zones present",
    )


def run_dataframe_suite_on_staging_partition(year: int, month: int) -> QualitySuiteResult:
    """Same logical gates as ``run_dataframe_suite`` against landed staging rows."""
    suite = QualitySuiteResult(
        results=[
            check_staging_nulls_partition(year, month),
            check_staging_temporal_order_partition(year, month),
            check_staging_non_negative_fare_partition(year, month),
            check_staging_distinct_zones_partition(year, month),
        ]
    )
    _log_suite("dataframe_staging", suite)
    return suite


def run_warehouse_suite(year: int, month: int, expected_min: int) -> QualitySuiteResult:
    suite = QualitySuiteResult(
        results=[
            check_warehouse_row_count(year, month, expected_min),
            check_warehouse_unique_trip_id(year, month),
        ]
    )
    _log_suite("warehouse", suite)
    return suite


def _log_suite(label: str, suite: QualitySuiteResult) -> None:
    for r in suite.results:
        log.info(
            "[quality:%s] %s passed=%s severity=%s metric=%s threshold=%s :: %s",
            label,
            r.name,
            r.passed,
            r.severity,
            r.metric,
            r.threshold,
            r.detail,
        )


def assert_suite_passed(suite: QualitySuiteResult, *, label: str = "quality") -> None:
    if suite.failed:
        msg = "; ".join(f"{r.name}: {r.detail}" for r in suite.failed)
        raise QualityCheckFailure(f"{label} suite failed -> {msg}")
