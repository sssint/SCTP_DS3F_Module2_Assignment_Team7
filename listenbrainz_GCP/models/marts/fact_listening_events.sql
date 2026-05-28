WITH listens AS (

    SELECT *
    FROM {{ ref('stg_listenbrainz_listens') }}

),

users AS (

    SELECT *
    FROM {{ ref('dim_user') }}

),

tracks AS (

    SELECT *
    FROM {{ ref('dim_track') }}

),

artists AS (

    SELECT *
    FROM {{ ref('dim_artist') }}

),

albums AS (

    SELECT *
    FROM {{ ref('dim_album') }}

)

SELECT
    l.listen_id,

    u.user_id,
    t.track_id,
    ar.artist_id,
    al.album_id,

    CAST(FORMAT_DATE('%Y%m%d', l.listen_date) AS INT64) AS date_id,
    l.listen_hour * 100 + l.listen_minute AS time_id,

    l.listened_at,

    1 AS listen_count

FROM listens l

LEFT JOIN users u
    ON l.user_name = u.user_name

LEFT JOIN tracks t
    ON l.track_name = t.track_name
    AND l.artist_name = t.artist_name

LEFT JOIN artists ar
    ON l.artist_name = ar.artist_name

LEFT JOIN albums al
    ON l.album_name = al.album_name
    AND l.artist_name = al.artist_name