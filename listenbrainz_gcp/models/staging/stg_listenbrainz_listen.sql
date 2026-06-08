WITH source_data AS (

    SELECT
        listened_at,
        user_name,

        artist_msid,
        TRIM(artist_name) AS artist_name,
        artist_mbids,

        release_msid,
        release_name,
        release_mbid,

        recording_msid,
        recording_mbid,
        TRIM(track_name) AS track_name,

        tags,

        DATE(listened_at) AS listen_date,
        EXTRACT(HOUR FROM listened_at) AS listen_hour,
        EXTRACT(MINUTE FROM listened_at) AS listen_minute

    FROM {{ source('listenbrainz_raw', 'listen') }}

    WHERE listened_at IS NOT NULL
      AND user_name IS NOT NULL
      AND track_name IS NOT NULL
      AND artist_name IS NOT NULL
      AND recording_msid IS NOT NULL
      AND TRIM(track_name) != ''
      AND TRIM(artist_name) != ''

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                listened_at,
                user_name,
                recording_msid,
                track_name,
                artist_name
            ORDER BY
                listened_at,
                user_name,
                recording_msid
        ) AS duplicate_row_number
    FROM source_data

),

final AS (

    SELECT
        TO_HEX(MD5(CONCAT(
            CAST(listened_at AS STRING),
            '|',
            user_name,
            '|',
            recording_msid,
            '|',
            track_name,
            '|',
            artist_name,
            '|',
            CAST(duplicate_row_number AS STRING)
        ))) AS listen_id,

        listened_at,
        user_name,

        artist_msid,
        artist_name,
        artist_mbids,

        release_msid,
        release_name,
        release_mbid,

        recording_msid,
        recording_mbid,
        track_name,

        tags,

        listen_date,
        listen_hour,
        listen_minute

    FROM deduplicated

)

SELECT *
FROM final
