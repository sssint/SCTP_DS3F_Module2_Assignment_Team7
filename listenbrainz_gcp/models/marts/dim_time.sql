SELECT DISTINCT
    listen_hour * 100 + listen_minute AS time_id,
    listen_hour AS hour,
    listen_minute AS minute,
    CASE
        WHEN listen_hour BETWEEN 5 AND 11 THEN 'Morning'
        WHEN listen_hour BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN listen_hour BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END AS time_period
FROM {{ ref('stg_listenbrainz_listen') }}
WHERE listen_hour IS NOT NULL