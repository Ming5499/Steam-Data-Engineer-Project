SELECT cs.game_appid
FROM {{ ref('stg_crawl_state') }} cs
LEFT JOIN {{ ref('stg_games') }} g ON cs.game_appid = g.game_id
WHERE g.game_id IS NULL