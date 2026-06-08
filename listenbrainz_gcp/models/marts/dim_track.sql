WITH source_data AS (

    SELECT
        TO_HEX(MD5(CONCAT(
            track_name,
            '|',
            recording_msid
        ))) AS track_id,

        track_name,
        recording_msid,
        recording_mbid,
        artist_name,
        release_name

    FROM {{ ref('stg_listenbrainz_listen') }}

    WHERE track_name IS NOT NULL
      AND recording_msid IS NOT NULL

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY track_id
            ORDER BY track_name
        ) AS row_number
    FROM source_data

)

SELECT
    track_id,
    track_name,
    recording_msid,
    recording_mbid,
    artist_name,
    release_name

FROM deduplicated
WHERE row_number = 1