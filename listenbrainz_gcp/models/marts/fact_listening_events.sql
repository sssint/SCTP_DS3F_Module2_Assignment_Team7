WITH listens AS (

    SELECT *
    FROM {{ ref('stg_listenbrainz_listen') }}

)

SELECT
    listen_id,

    TO_HEX(MD5(user_name)) AS user_id,

    TO_HEX(MD5(CONCAT(
        track_name,
        '|',
        recording_msid
    ))) AS track_id,

    TO_HEX(MD5(CONCAT(
        artist_name,
        '|',
        IFNULL(artist_mbids, '')
    ))) AS artist_id,

    TO_HEX(MD5(CONCAT(
        IFNULL(release_name, ''),
        '|',
        IFNULL(artist_name, '')
    ))) AS album_id,

    CAST(FORMAT_DATE('%Y%m%d', listen_date) AS INT64) AS date_id,

    listen_hour * 100 + listen_minute AS time_id,

    listened_at,

    1 AS listen_count

FROM listens