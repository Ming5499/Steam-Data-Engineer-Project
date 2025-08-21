{{ config(materialized='view') }}

SELECT
    game_appid,
    last_review_timestamp,
    last_price_timestamp
FROM {{ source('steam_db', 'crawl_state') }}