-- Upsert observed pickup/dropoff zones into the dim_zone table.
-- Real deployments would join against the TLC zone lookup file; here we keep
-- it self-contained so the pipeline can run with only trip data.

INSERT INTO marts.dim_zone (zone_id, borough, zone_name, service_zone, last_seen_at)
SELECT DISTINCT
    z.zone_id,
    'unknown'::text                                          AS borough,
    ('Zone ' || z.zone_id::text)                             AS zone_name,
    'unknown'::text                                          AS service_zone,
    NOW()                                                    AS last_seen_at
FROM (
    SELECT pickup_zone_id  AS zone_id FROM staging.fct_trips WHERE trip_year = :year AND trip_month = :month
    UNION
    SELECT dropoff_zone_id AS zone_id FROM staging.fct_trips WHERE trip_year = :year AND trip_month = :month
) z
ON CONFLICT (zone_id) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at;
