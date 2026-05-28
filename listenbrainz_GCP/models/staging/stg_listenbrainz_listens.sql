WITH source_data AS (

    SELECT
        -- event information
        listen_id,
        user_name,
        listened_at,

        -- track information
        track_name,
        artist_name,
        album_name,
        recording_mbid,
        artist_mbid,

        -- derived fields
        DATE(listened_at) AS listen_date,
        EXTRACT(HOUR FROM listened_at) AS listen_hour,
        EXTRACT(MINUTE FROM listened_at) AS listen_minute

    FROM {{ source('listenbrainz_raw', 'listens') }}

    WHERE listened_at IS NOT NULL
)

SELECT *
FROM source_data