
SELECT
    {{ dbt_utils.generate_surrogate_key(['track_name', 'artist_name', 'recording_mbid']) }} AS track_id,
    track_name,
    artist_name,
    album_name,
    recording_mbid
FROM {{ ref('stg_listenbrainz_listens') }}
WHERE track_name IS NOT NULL
GROUP BY track_name, artist_name, album_name, recording_mbid