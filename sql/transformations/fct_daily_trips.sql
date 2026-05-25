-- Build / refresh the daily aggregate for the requested partition.
-- Parameters: :year, :month
-- Idempotent: deletes existing rows for the partition before re-inserting.

DELETE FROM marts.fct_daily_trips
WHERE trip_date >= make_date(:year, :month, 1)
  AND trip_date <  (make_date(:year, :month, 1) + INTERVAL '1 month')::date;

INSERT INTO marts.fct_daily_trips (
    trip_date, pickup_zone_id, trip_count, total_passengers,
    total_revenue, avg_trip_distance, avg_fare, p95_fare, refreshed_at
)
SELECT
    pickup_ts::date                                    AS trip_date,
    pickup_zone_id                                     AS pickup_zone_id,
    COUNT(*)                                           AS trip_count,
    SUM(passenger_count)::bigint                       AS total_passengers,
    SUM(total_amount)::numeric(14,2)                   AS total_revenue,
    AVG(trip_distance_mi)::numeric(8,3)                AS avg_trip_distance,
    AVG(fare_amount)::numeric(10,2)                    AS avg_fare,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fare_amount)::numeric(10,2) AS p95_fare,
    NOW()                                              AS refreshed_at
FROM staging.fct_trips
WHERE trip_year = :year AND trip_month = :month
GROUP BY pickup_ts::date, pickup_zone_id;
