-- Append a row-count snapshot to ops.row_count_history.
-- Parameters: :pipeline, :year, :month, :row_count, :historical_median,
--             :deviation_pct, :is_anomaly, :reason

INSERT INTO ops.row_count_history (
    pipeline, trip_year, trip_month, snapshot_at,
    row_count, historical_median, deviation_pct, is_anomaly, reason
) VALUES (
    :pipeline, :year, :month, NOW(),
    :row_count, :historical_median, :deviation_pct, :is_anomaly, :reason
);
