WITH source_data AS (

    SELECT
        TO_HEX(MD5(CONCAT(
            artist_name,
            '|',
            IFNULL(artist_mbids, '')
        ))) AS artist_id,

        artist_name,
        artist_mbids

    FROM {{ ref('stg_listenbrainz_listen') }}

    WHERE artist_name IS NOT NULL

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY artist_id
            ORDER BY artist_name
        ) AS row_number
    FROM source_data

)

SELECT
    artist_id,
    artist_name,
    artist_mbids

FROM deduplicated
WHERE row_number = 1