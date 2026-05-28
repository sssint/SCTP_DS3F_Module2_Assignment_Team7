SELECT
    {{ dbt_utils.generate_surrogate_key(['album_name', 'artist_name']) }} AS album_id,
    album_name,
    artist_name
FROM {{ ref('stg_listenbrainz_listens') }}
WHERE album_name IS NOT NULL
GROUP BY album_name, artist_name