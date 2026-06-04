WITH source_data AS (

    SELECT
        -- create unique ID because raw table has no listen_id
        {{ dbt_utils.generate_surrogate_key([
            'CAST(listened_at AS STRING)',
            'user_name',
            'recording_msid'
        ]) }} AS listen_id,

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

        DATE(listened_at) AS listen_date,
        EXTRACT(HOUR FROM listened_at) AS listen_hour,
        EXTRACT(MINUTE FROM listened_at) AS listen_minute

    FROM {{ source('listenbrainz_raw', 'listen') }}

    WHERE listened_at IS NOT NULL
      AND user_name IS NOT NULL
      AND track_name IS NOT NULL
      AND artist_name IS NOT NULL
)

SELECT *
FROM source_data