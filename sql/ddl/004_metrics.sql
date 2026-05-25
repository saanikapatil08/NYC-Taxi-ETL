-- Operational metrics: pipeline run history + per-partition row counts used by
-- the row-count anomaly detector and the monitoring DAG.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    pipeline        TEXT NOT NULL,
    run_key         TEXT NOT NULL,
    trip_year       INTEGER NOT NULL,
    trip_month      INTEGER NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running',
    rows_loaded     BIGINT,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_partition
    ON ops.pipeline_runs (pipeline, trip_year, trip_month);

CREATE TABLE IF NOT EXISTS ops.row_count_history (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    pipeline        TEXT NOT NULL,
    trip_year       INTEGER NOT NULL,
    trip_month      INTEGER NOT NULL,
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    row_count       BIGINT NOT NULL,
    historical_median NUMERIC,
    deviation_pct   NUMERIC,
    is_anomaly      BOOLEAN NOT NULL DEFAULT FALSE,
    reason          TEXT
);

CREATE INDEX IF NOT EXISTS ix_row_count_history_partition
    ON ops.row_count_history (pipeline, trip_year, trip_month);
