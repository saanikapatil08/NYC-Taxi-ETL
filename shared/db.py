"""Database helpers built on SQLAlchemy and psycopg2.

Both orchestrators rely on the same connection plumbing so that a single
``DATABASE_URL`` change propagates everywhere.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection as PgConnection
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from shared.config import get_settings
from shared.logging_config import get_logger

log = get_logger(__name__)


def get_engine() -> Engine:
    """Return a SQLAlchemy engine bound to the warehouse Postgres."""
    settings = get_settings()
    return create_engine(
        settings.sqlalchemy_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        future=True,
    )


@contextmanager
def pg_connection() -> Iterator[PgConnection]:
    """Yield a psycopg2 connection. Caller is responsible for commit semantics."""
    settings = get_settings()
    conn = psycopg2.connect(settings.psycopg_dsn)
    try:
        yield conn
    finally:
        conn.close()


def run_sql_file(path: str | Path) -> None:
    """Execute every statement in a .sql file against the warehouse."""
    sql = Path(path).read_text(encoding="utf-8")
    log.info("Executing SQL file %s", path)
    engine = get_engine()
    with engine.begin() as conn:
        for stmt in _split_statements(sql):
            if stmt.strip():
                conn.execute(text(stmt))


def _split_statements(sql: str) -> list[str]:
    """Naive SQL splitter that respects $$ blocks used by Postgres functions."""
    parts: list[str] = []
    buf: list[str] = []
    in_dollar = False
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        if "$$" in line:
            in_dollar = not in_dollar
        buf.append(line)
        if not in_dollar and stripped.endswith(";"):
            parts.append("\n".join(buf))
            buf = []
    if buf:
        parts.append("\n".join(buf))
    return parts


def table_row_count(schema: str, table: str) -> int:
    """Return the row count for a fully-qualified table."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"'))
        return int(result.scalar() or 0)
