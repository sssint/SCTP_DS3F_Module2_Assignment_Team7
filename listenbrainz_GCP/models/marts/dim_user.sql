
SELECT
    {{ dbt_utils.generate_surrogate_key(['user_name']) }} AS user_id,
    user_name
FROM {{ ref('stg_listenbrainz_listens') }}
WHERE user_name IS NOT NULL
GROUP BY user_name