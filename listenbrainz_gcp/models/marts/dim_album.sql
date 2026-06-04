SELECT
    {{ dbt_utils.generate_surrogate_key(['release_name', 'artist_name']) }} AS album_id,
    release_name,
    artist_name
FROM {{ ref('stg_listenbrainz_listen') }}
WHERE release_name IS NOT NULL
GROUP BY release_name, artist_name