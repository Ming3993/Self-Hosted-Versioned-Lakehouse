CREATE TABLE iceberg.events.fact_page_views 
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(view_time)']
) AS
SELECT
    sessionid AS session_id,
    userid AS user_id,
    page AS page_name,
    method,
    status AS http_status,
    from_unixtime(CAST(ts AS DOUBLE) / 1000) AS view_time
FROM iceberg.events.page_view_events
WHERE userid IS NOT NULL;