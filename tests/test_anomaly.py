"""Tests for the row-count anomaly detector."""

from __future__ import annotations

import pytest

from shared.anomaly import RowCountAnomaly, assert_no_anomaly, detect_row_count_anomaly


def test_detector_flags_below_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MIN_EXPECTED_ROWS", "1000")
    from shared.config import get_settings

    get_settings.cache_clear()

    result = detect_row_count_anomaly(
        2024, 3, current_rows=10, historical=[100_000, 110_000, 105_000]
    )
    assert result.is_anomaly is True
    assert "below absolute floor" in result.reason


def test_detector_flags_large_deviation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ROW_COUNT_DEVIATION_PCT", "0.20")
    monkeypatch.setenv("MIN_EXPECTED_ROWS", "10")
    from shared.config import get_settings

    get_settings.cache_clear()

    result = detect_row_count_anomaly(
        2024, 3, current_rows=200, historical=[1000, 1010, 990]
    )
    assert result.is_anomaly is True
    assert result.deviation_pct is not None and result.deviation_pct > 0.2


def test_detector_passes_within_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ROW_COUNT_DEVIATION_PCT", "0.30")
    monkeypatch.setenv("MIN_EXPECTED_ROWS", "10")
    from shared.config import get_settings

    get_settings.cache_clear()

    result = detect_row_count_anomaly(
        2024, 3, current_rows=1050, historical=[1000, 1010, 990]
    )
    assert result.is_anomaly is False


def test_assert_no_anomaly_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MIN_EXPECTED_ROWS", "100")
    from shared.config import get_settings

    get_settings.cache_clear()

    bad = detect_row_count_anomaly(2024, 3, current_rows=10, historical=[100, 110, 90])
    with pytest.raises(RowCountAnomaly):
        assert_no_anomaly(bad)


def test_no_history_does_not_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MIN_EXPECTED_ROWS", "10")
    from shared.config import get_settings

    get_settings.cache_clear()

    result = detect_row_count_anomaly(2024, 3, current_rows=500, historical=[])
    assert result.is_anomaly is False
    assert "no historical partitions" in result.reason
