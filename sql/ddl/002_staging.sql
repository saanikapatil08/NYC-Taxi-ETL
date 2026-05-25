-- Staging: cleaned, conformed trip facts. Loaded by shared.load.load_staging.
-- Idempotency is enforced by deleting the (trip_year, trip_month) partition
-- before reload.

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.fct_trips (
    trip_id              BIGSERIAL PRIMARY KEY,
    vendor_id            INTEGER NOT NULL,
    pickup_ts            TIMESTAMP NOT NULL,
    dropoff_ts           TIMESTAMP NOT NULL,
    passenger_count      INTEGER NOT NULL CHECK (passenger_count >= 1),
    trip_distance_mi     NUMERIC(8,3) NOT NULL CHECK (trip_distance_mi > 0),
    trip_duration_min    NUMERIC(8,2) NOT NULL CHECK (trip_duration_min > 0),
    pickup_zone_id       INTEGER NOT NULL,
    dropoff_zone_id      INTEGER NOT NULL,
    payment_type         INTEGER NOT NULL,
    fare_amount          NUMERIC(10,2) NOT NULL CHECK (fare_amount >= 0),
    tip_amount           NUMERIC(10,2) NOT NULL CHECK (tip_amount >= 0),
    total_amount         NUMERIC(10,2) NOT NULL CHECK (total_amount >= 0),
    trip_year            INTEGER NOT NULL,
    trip_month           INTEGER NOT NULL CHECK (trip_month BETWEEN 1 AND 12),
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_fct_trips_partition
    ON staging.fct_trips (trip_year, trip_month);

CREATE INDEX IF NOT EXISTS ix_fct_trips_pickup_ts
    ON staging.fct_trips (pickup_ts);

CREATE INDEX IF NOT EXISTS ix_fct_trips_pickup_zone
    ON staging.fct_trips (pickup_zone_id);
