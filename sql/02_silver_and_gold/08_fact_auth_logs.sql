CREATE TABLE iceberg.events.fact_auth_logs 
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(auth_time)']
) AS
SELECT
    sessionid AS session_id,
    userid AS user_id,
    success AS is_success,
    from_unixtime(CAST(ts AS DOUBLE) / 1000) AS auth_time,
    city,
    state
FROM iceberg.events.auth_events;