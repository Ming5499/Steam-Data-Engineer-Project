{{ config(materialized='table') }}

SELECT
    r.appid,
    r.author_steamid,
    r.review,
    FROM_UNIXTIME(r.timestamp_created) AS review_timestamp,
    r.language,
    COUNT(*) OVER (PARTITION BY r.appid) AS review_count
FROM {{ ref('stg_reviews') }} r