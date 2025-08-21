SELECT cs.game_appid
FROM `steam_db_staging`.`stg_crawl_state` cs
LEFT JOIN `steam_db_staging`.`stg_games` g ON cs.game_appid = g.game_id
WHERE g.game_id IS NULL