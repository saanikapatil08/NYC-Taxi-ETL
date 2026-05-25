"""Idempotent loaders for raw and staging trip data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import text

from shared.config import get_settings
from shared.db import get_engine, pg_connection, run_sql_file
from shared.logging_config import get_logger
from shared.schema import validate_raw, validate_staging
from shared.transform import clean_trips

log = get_logger(__name__)

DDL_FILES = (
    "sql/ddl/001_raw.sql",
    "sql/ddl/002_staging.sql",
    "sql/ddl/003_marts.sql",
    "sql/ddl/004_metrics.sql",
)


def _default_project_root() -> Path:
    """Repo root (parent of ``shared``), regardless of process cwd."""
    return Path(__file__).resolve().parents[1]


def init_warehouse(project_root: str | Path | None = None) -> None:
    """Apply every DDL file in order. Safe to run repeatedly."""
    root = Path(project_root) if project_root is not None else _default_project_root()
    for rel in DDL_FILES:
        run_sql_file(root / rel)
    log.info("Warehouse DDL applied")


_STAGING_TRIP_COLS = (
    "vendor_id",
    "pickup_ts",
    "dropoff_ts",
    "passenger_count",
    "trip_distance_mi",
    "trip_duration_min",
    "pickup_zone_id",
    "dropoff_zone_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "trip_year",
    "trip_month",
)


def bulk_append_staging_rows(df: pd.DataFrame) -> None:
    """Append staging-shaped rows via psycopg2 (avoids pandas/SQLAlchemy to_sql issues)."""
    if df.empty:
        return
    from psycopg2.extras import execute_values

    cols = _STAGING_TRIP_COLS
    rows = list(zip(*(df[c] for c in cols)))
    stmt = (
        "INSERT INTO staging.fct_trips ("
        + ", ".join(cols)
        + ") VALUES %s"
    )
    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, stmt, rows, page_size=10_000)
        conn.commit()


def read_raw_parquet(path: str | Path) -> pd.DataFrame:
    """Load a raw TLC parquet file into a pandas DataFrame and validate it."""
    df = pq.read_table(str(path)).to_pandas()
    validate_raw(df)
    return df


def validate_raw_parquet_file(path: str | Path, *, batch_size: int = 65_536) -> int:
    """Run raw Pandera validation over parquet batches; return total row count."""
    pf = pq.ParquetFile(str(path))
    total = pf.metadata.num_rows
    for batch in pf.iter_batches(batch_size=batch_size):
        validate_raw(batch.to_pandas())
    return total


def stream_clean_load_staging(
    path: str | Path, *, year: int, month: int, batch_size: int = 65_536
) -> int:
    """Validate, clean, and load one partition without holding the full month in RAM."""
    init_warehouse()
    pf = pq.ParquetFile(str(path))
    engine = get_engine()
    rows_written = 0

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM staging.fct_trips
                WHERE trip_year = :year AND trip_month = :month
                """
            ),
            {"year": year, "month": month},
        )

    insert_batch = 50_000
    for batch in pf.iter_batches(batch_size=batch_size):
        raw_chunk = batch.to_pandas()
        validate_raw(raw_chunk)
        cleaned = clean_trips(raw_chunk, year=year, month=month)
        if cleaned.empty:
            continue
        validate_staging(cleaned)
        for start in range(0, len(cleaned), insert_batch):
            sub = cleaned.iloc[start : start + insert_batch]
            bulk_append_staging_rows(sub)
            rows_written += len(sub)
        log.info(
            "stream_clean_load_staging: loaded %d rows cumulatively for %04d-%02d",
            rows_written,
            year,
            month,
        )

    log.info(
        "stream_clean_load_staging complete: %d rows for %04d-%02d into %s",
        rows_written,
        year,
        month,
        get_settings().postgres_db,
    )
    return rows_written


def load_staging(df: pd.DataFrame, *, year: int, month: int, batch_size: int = 50_000) -> int:
    """Replace the staging partition for ``year``/``month`` and return rows written."""
    settings = get_settings()
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM staging.fct_trips
                WHERE trip_year = :year AND trip_month = :month
                """
            ),
            {"year": year, "month": month},
        )

    rows_written = 0
    for start in range(0, len(df), batch_size):
        chunk = df.iloc[start : start + batch_size]
        bulk_append_staging_rows(chunk)
        rows_written += len(chunk)
        log.info("Loaded %d/%d rows into staging.fct_trips", rows_written, len(df))

    log.info(
        "load_staging complete: %d rows for %04d-%02d into %s",
        rows_written,
        year,
        month,
        settings.postgres_db,
    )
    return rows_written


def execute_transformation_sql(path: str | Path, params: dict | None = None) -> None:
    """Run a templated SQL transformation file (single statement preferred)."""
    sql = Path(path).read_text(encoding="utf-8")
    log.info("Running transformation SQL %s", path)
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})
