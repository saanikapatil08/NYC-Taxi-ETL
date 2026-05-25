"""Smoke-test alerting: ensure send_alert never raises and respects config."""

from __future__ import annotations

import pytest

from shared.alerts import AlertPayload, send_alert


def test_send_alert_is_safe_without_webhook(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    from shared.config import get_settings

    get_settings.cache_clear()

    send_alert(
        AlertPayload(
            title="t",
            severity="error",
            pipeline="p",
            component="c",
            message="m",
            context={"k": "v"},
        )
    )


def test_send_alert_swallows_http_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "http://invalid.local/hook")
    from shared.config import get_settings

    get_settings.cache_clear()

    def boom(*_a, **_kw):
        raise __import__("requests").RequestException("nope")

    import shared.alerts as alerts_mod

    monkeypatch.setattr(alerts_mod.requests, "post", boom)
    send_alert(
        AlertPayload(
            title="t",
            severity="warning",
            pipeline="p",
            component="c",
            message="m",
            context={"k": "v"},
        )
    )
