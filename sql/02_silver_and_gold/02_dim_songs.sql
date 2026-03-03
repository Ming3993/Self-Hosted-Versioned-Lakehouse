CREATE TABLE iceberg.events.dim_songs 
WITH (format = 'PARQUET') AS
SELECT DISTINCT
    to_hex(md5(to_utf8(artist || song))) AS song_id,
    artist AS artist_name,
    song AS song_title,
    CAST(duration AS DOUBLE) AS duration
FROM iceberg.events.listen_events
WHERE artist IS NOT NULL AND song IS NOT NULL;