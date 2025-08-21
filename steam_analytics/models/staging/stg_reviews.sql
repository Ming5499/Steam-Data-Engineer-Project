{{ config(materialized='table') }}

{% set results = run_query(get_reviews()) %}
SELECT
    CAST(appid AS INTEGER) AS appid,
    author_steamid,
    review,
    FROM_UNIXTIME(timestamp_created) AS review_timestamp,
    language
FROM {{ results }}