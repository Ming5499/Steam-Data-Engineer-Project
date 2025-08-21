SELECT game_id, COUNT(*) as count
FROM `steam_db_staging`.`stg_games`
GROUP BY game_id
HAVING count > 1