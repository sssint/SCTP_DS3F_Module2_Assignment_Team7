SELECT DISTINCT
    CAST(FORMAT_DATE('%Y%m%d', listen_date) AS INT64) AS date_id,
    listen_date AS full_date,
    EXTRACT(DAY FROM listen_date) AS day,
    EXTRACT(MONTH FROM listen_date) AS month,
    FORMAT_DATE('%B', listen_date) AS month_name,
    EXTRACT(QUARTER FROM listen_date) AS quarter,
    EXTRACT(YEAR FROM listen_date) AS year,
    FORMAT_DATE('%A', listen_date) AS day_of_week,
    CASE
        WHEN FORMAT_DATE('%A', listen_date) IN ('Saturday', 'Sunday') THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM {{ ref('stg_listenbrainz_listen') }}
WHERE listen_date IS NOT NULL