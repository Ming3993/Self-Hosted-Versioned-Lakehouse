CREATE TABLE iceberg.events.fact_subscriptions 
WITH (format = 'PARQUET') AS
SELECT
    sessionid AS session_id,
    userid AS user_id,
    level AS subscription_level, -- 'free' hoặc 'paid'
    from_unixtime(CAST(ts AS DOUBLE) / 1000) AS change_time
FROM iceberg.events.status_change_events
WHERE userid IS NOT NULL;