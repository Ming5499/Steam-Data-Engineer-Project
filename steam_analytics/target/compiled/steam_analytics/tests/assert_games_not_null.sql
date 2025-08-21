SELECT *
FROM `steam_db_staging`.`stg_games`
WHERE game_id IS NULL OR title IS NULL