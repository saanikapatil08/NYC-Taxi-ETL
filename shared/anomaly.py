"""Row-count anomaly detection.

Compares the row count for a given partition against the trailing median of
prior partitions. If the deviation exceeds the configured threshold, or if the
absolute count falls below ``min_expected_rows``, an anomaly is raised.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass

from sqlalchemy import text

from shared.config import get_settings
from shared.db import get_engine
from shared.logging_config import get_logger

log = get_logger(__name__)


class RowCountAnomaly(RuntimeError):
    """Raised when an anomaly is detected and we want to fail the pipeline."""


@dataclass
class AnomalyResult:
    year: int
    month: int
    current_rows: int
    historical_median: float | None
    deviation_pct: float | None
    is_anomaly: bool
    reason: str


def fetch_current_count(year: int, month: int) -> int:
    engine = get_engine()
    with engine.connect() as conn:
        n = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM staging.fct_trips
                WHERE trip_year = :year AND trip_month = :month
                """
            ),
            {"year": year, "month": month},
        ).scalar()
    return int(n or 0)


def fetch_historical_counts(year: int, month: int, lookback: int = 6) -> list[int]:
    """Return row counts for the ``lookback`` partitions immediately before (year, month)."""
    pairs: list[tuple[int, int]] = []
    y, m = year, month
    for _ in range(lookback):
        m -= 1
        if m == 0:
            m = 12
            y -= 1
        pairs.append((y, m))

    engine = get_engine()
    counts: list[int] = []
    with engine.connect() as conn:
        for py, pm in pairs:
            n = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM staging.fct_trips
                    WHERE trip_year = :year AND trip_month = :month
                    """
                ),
                {"year": py, "month": pm},
            ).scalar()
            counts.append(int(n or 0))
    return [c for c in counts if c > 0]


def detect_row_count_anomaly(
    year: int,
    month: int,
    *,
    current_rows: int | None = None,
    historical: list[int] | None = None,
) -> AnomalyResult:
    """Pure detector that can be unit-tested with explicit inputs."""
    settings = get_settings()
    if current_rows is None:
        current_rows = fetch_current_count(year, month)
    if historical is None:
        historical = fetch_historical_counts(year, month)

    if current_rows < settings.min_expected_rows:
        return AnomalyResult(
            year=year,
            month=month,
            current_rows=current_rows,
            historical_median=None,
            deviation_pct=None,
            is_anomaly=True,
            reason=(
                f"row count {current_rows} is below absolute floor "
                f"min_expected_rows={settings.min_expected_rows}"
            ),
        )

    if not historical:
        return AnomalyResult(
            year=year,
            month=month,
            current_rows=current_rows,
            historical_median=None,
            deviation_pct=None,
            is_anomaly=False,
            reason="no historical partitions available; skipping deviation check",
        )

    median = statistics.median(historical)
    if median == 0:
        return AnomalyResult(
            year=year,
            month=month,
            current_rows=current_rows,
            historical_median=median,
            deviation_pct=None,
            is_anomaly=current_rows == 0,
            reason="historical median is zero",
        )

    deviation = abs(current_rows - median) / median
    is_anomaly = deviation > settings.row_count_deviation_pct
    reason = (
        f"deviation {deviation:.2%} vs threshold {settings.row_count_deviation_pct:.0%} "
        f"(current={current_rows}, median={median:.0f})"
    )
    return AnomalyResult(
        year=year,
        month=month,
        current_rows=current_rows,
        historical_median=median,
        deviation_pct=deviation,
        is_anomaly=is_anomaly,
        reason=reason,
    )


def assert_no_anomaly(result: AnomalyResult) -> None:
    if result.is_anomaly:
        raise RowCountAnomaly(
            f"Row-count anomaly detected for {result.year:04d}-{result.month:02d}: "
            f"{result.reason}"
        )
