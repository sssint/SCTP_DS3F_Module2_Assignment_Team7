SELECT
    {{ dbt_utils.generate_surrogate_key(['artist_name', 'artist_mbids']) }} AS artist_id,
    artist_name,
    artist_mbids
FROM {{ ref('stg_listenbrainz_listen') }}
WHERE artist_name IS NOT NULL
GROUP BY artist_name, artist_mbids