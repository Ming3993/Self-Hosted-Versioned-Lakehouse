CREATE TABLE iceberg.events.dim_users 
WITH (format = 'PARQUET') AS
SELECT 
    userid AS user_id,
    MAX(firstname) AS first_name,
    MAX(lastname) AS last_name,
    MAX(gender) AS gender,
    -- Lấy thời gian đăng ký (chuyển từ milli-giây sang Timestamp)
    from_unixtime(MAX(CAST(registration AS DOUBLE)) / 1000) AS registration_time
FROM iceberg.events.auth_events
WHERE userid IS NOT NULL
GROUP BY userid;