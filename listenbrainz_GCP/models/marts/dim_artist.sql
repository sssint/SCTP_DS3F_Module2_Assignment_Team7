SELECT
    {{ dbt_utils.generate_surrogate_key(['artist_name', 'artist_mbid']) }} AS artist_id,
    artist_name,
    artist_mbid
FROM {{ ref('stg_listenbrainz_listens') }}
WHERE artist_name IS NOT NULL
GROUP BY artist_name, artist_mbid