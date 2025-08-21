{{ config(materialized='table') }}

SELECT
    dg.game_id,
    dg.title,
    dg.developers,
    dg.genres,
    fr.review_count,
    AVG(p.price) AS avg_price,
    AVG(p.discount) AS avg_discount,
    MIN(p.initial_price) AS min_initial_price
FROM {{ ref('dim_games') }} dg
LEFT JOIN {{ ref('fact_reviews') }} fr ON dg.game_id = fr.appid
LEFT JOIN {{ ref('stg_prices') }} p ON dg.game_id = p.game_id
GROUP BY dg.game_id, dg.title, dg.developers, dg.genres, fr.review_count