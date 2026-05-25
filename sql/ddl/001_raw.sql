-- Raw landing schema. Mirrors the TLC parquet feed; nothing is enforced here so
-- that schema drift does not block ingestion. Validation happens in pandera.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.ingest_log (
    ingest_id        BIGSERIAL PRIMARY KEY,
    pipeline         TEXT NOT NULL,
    dataset          TEXT NOT NULL,
    trip_year        INTEGER NOT NULL,
    trip_month       INTEGER NOT NULL,
    source_url       TEXT NOT NULL,
    local_path       TEXT NOT NULL,
    bytes_written    BIGINT NOT NULL,
    rows_observed    BIGINT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    status           TEXT NOT NULL DEFAULT 'started'
);

CREATE INDEX IF NOT EXISTS ix_ingest_log_partition
    ON raw.ingest_log (trip_year, trip_month);
