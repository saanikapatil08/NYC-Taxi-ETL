"""Centralised configuration loaded from environment variables.

Both the Airflow DAG and the Dagster definitions resolve their settings through
``get_settings`` so that swapping environments (local Docker, staging, prod)
only requires changing env vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    # Postgres warehouse
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    # Pipeline knobs
    taxi_data_base_url: str
    taxi_dataset: str
    data_dir: str
    log_level: str

    # Anomaly thresholds
    row_count_deviation_pct: float
    min_expected_rows: int

    # Alerting
    slack_webhook_url: str
    alert_email_to: str
    alert_email_from: str

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def psycopg_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


def _env(name: str, default: str | None = None, *, required: bool = False) -> str:
    val = os.environ.get(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Required environment variable {name} is not set")
    return val if val is not None else ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        postgres_host=_env("POSTGRES_HOST", "localhost"),
        postgres_port=int(_env("POSTGRES_PORT", "5432")),
        postgres_db=_env("POSTGRES_DB", "taxi"),
        postgres_user=_env("POSTGRES_USER", "taxi"),
        postgres_password=_env("POSTGRES_PASSWORD", "taxi"),
        taxi_data_base_url=_env(
            "TAXI_DATA_BASE_URL",
            "https://d37ci6vzurychx.cloudfront.net/trip-data",
        ),
        taxi_dataset=_env("TAXI_DATASET", "yellow_tripdata"),
        data_dir=_env("DATA_DIR", "./data"),
        log_level=_env("LOG_LEVEL", "INFO"),
        row_count_deviation_pct=float(_env("ROW_COUNT_DEVIATION_PCT", "0.30")),
        min_expected_rows=int(_env("MIN_EXPECTED_ROWS", "10000")),
        slack_webhook_url=_env("SLACK_WEBHOOK_URL", ""),
        alert_email_to=_env("ALERT_EMAIL_TO", ""),
        alert_email_from=_env("ALERT_EMAIL_FROM", ""),
    )
