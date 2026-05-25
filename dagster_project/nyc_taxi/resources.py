"""Dagster resources backed by the same shared modules used by Airflow."""

from __future__ import annotations

from dagster import ConfigurableResource
from sqlalchemy.engine import Engine

from shared.alerts import AlertPayload, send_alert
from shared.config import Settings, get_settings
from shared.db import get_engine


class WarehouseResource(ConfigurableResource):
    """Wraps the shared SQLAlchemy engine so assets can request it explicitly."""

    def get_engine(self) -> Engine:
        return get_engine()


class AlertResource(ConfigurableResource):
    """Thin wrapper around shared.alerts.send_alert."""

    pipeline: str = "dagster:nyc_taxi_etl"

    def alert(self, *, severity: str, component: str, title: str, message: str, **ctx) -> None:
        send_alert(
            AlertPayload(
                title=title,
                severity=severity,
                pipeline=self.pipeline,
                component=component,
                message=message,
                context=ctx,
            )
        )


class SettingsResource(ConfigurableResource):
    """Convenience accessor for the Settings dataclass."""

    def get(self) -> Settings:
        return get_settings()
