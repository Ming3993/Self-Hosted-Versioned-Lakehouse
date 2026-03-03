CREATE TABLE iceberg.events.dim_date 
WITH (format = 'PARQUET') AS
SELECT DISTINCT
    CAST(from_unixtime(CAST(ts AS DOUBLE) / 1000) AS DATE) AS date_id,
    EXTRACT(YEAR FROM from_unixtime(CAST(ts AS DOUBLE) / 1000)) AS year,
    EXTRACT(MONTH FROM from_unixtime(CAST(ts AS DOUBLE) / 1000)) AS month,
    EXTRACT(DAY FROM from_unixtime(CAST(ts AS DOUBLE) / 1000)) AS day,
    EXTRACT(DOW FROM from_unixtime(CAST(ts AS DOUBLE) / 1000)) AS day_of_week
FROM iceberg.events.listen_events;