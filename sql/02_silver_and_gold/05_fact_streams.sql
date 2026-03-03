CREATE TABLE iceberg.events.fact_streams 
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(play_time)']
) AS
SELECT
    sessionid AS session_id,
    userid AS user_id,
    to_hex(md5(to_utf8(artist || song))) AS song_id,
    from_unixtime(CAST(ts AS DOUBLE) / 1000) AS play_time,
    iteminsession AS item_in_session,
    auth,
    CAST(lat AS DOUBLE) AS lat,
    CAST(lon AS DOUBLE) AS lon
FROM iceberg.events.listen_events
WHERE artist IS NOT NULL AND song IS NOT NULL;