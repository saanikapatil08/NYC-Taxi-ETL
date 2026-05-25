-- Marts schema: business-ready aggregates served from staging.

CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS marts.dim_zone (
    zone_id          INTEGER PRIMARY KEY,
    borough          TEXT,
    zone_name        TEXT,
    service_zone     TEXT,
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS marts.fct_daily_trips (
    trip_date          DATE NOT NULL,
    pickup_zone_id     INTEGER NOT NULL,
    trip_count         BIGINT NOT NULL,
    total_passengers   BIGINT NOT NULL,
    total_revenue      NUMERIC(14,2) NOT NULL,
    avg_trip_distance  NUMERIC(8,3) NOT NULL,
    avg_fare           NUMERIC(10,2) NOT NULL,
    p95_fare           NUMERIC(10,2) NOT NULL,
    refreshed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trip_date, pickup_zone_id)
);

CREATE INDEX IF NOT EXISTS ix_fct_daily_trips_date
    ON marts.fct_daily_trips (trip_date);
