CREATE TABLE iceberg.events.dim_location 
WITH (format = 'PARQUET') AS
SELECT DISTINCT
    zip AS zip_code,
    city,
    state,
    CAST(lat AS DOUBLE) AS latitude,
    CAST(lon AS DOUBLE) AS longitude
FROM iceberg.events.auth_events
WHERE zip IS NOT NULL;