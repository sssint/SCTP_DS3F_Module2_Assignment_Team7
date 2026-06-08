WITH source_data AS (

    SELECT
        TO_HEX(MD5(CONCAT(
            IFNULL(release_name, ''),
            '|',
            IFNULL(artist_name, '')
        ))) AS album_id,

        release_name,
        artist_name

    FROM {{ ref('stg_listenbrainz_listen') }}

    WHERE release_name IS NOT NULL

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY album_id
            ORDER BY release_name
        ) AS row_number
    FROM source_data

)

SELECT
    album_id,
    release_name,
    artist_name

FROM deduplicated
WHERE row_number = 1