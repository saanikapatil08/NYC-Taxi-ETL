"""Alert dispatch shared between Airflow callbacks and Dagster sensors.

If ``SLACK_WEBHOOK_URL`` is configured the alert is POSTed there. In every
environment alerts are also written to the structured log so that the failure
shows up in the orchestrator's task log.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import requests

from shared.config import get_settings
from shared.logging_config import get_logger

log = get_logger(__name__)


@dataclass
class AlertPayload:
    title: str
    severity: str  # "info" | "warning" | "error"
    pipeline: str
    component: str
    message: str
    context: dict[str, Any]


def _format_text(payload: AlertPayload) -> str:
    bullets = "\n".join(f"  - {k}: {v}" for k, v in payload.context.items())
    return (
        f"[{payload.severity.upper()}] {payload.pipeline} :: {payload.component}\n"
        f"{payload.title}\n{payload.message}\n"
        f"context:\n{bullets}"
    )


def send_alert(payload: AlertPayload) -> None:
    """Best-effort alert delivery. Never raises."""
    text = _format_text(payload)
    if payload.severity == "error":
        log.error("ALERT %s", text)
    elif payload.severity == "warning":
        log.warning("ALERT %s", text)
    else:
        log.info("ALERT %s", text)

    settings = get_settings()
    if not settings.slack_webhook_url:
        return

    try:
        requests.post(
            settings.slack_webhook_url,
            data=json.dumps({"text": text}),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
    except requests.RequestException as exc:
        log.warning("Failed to POST alert to Slack: %s", exc)
