SELECT *
FROM `steam_db_staging`.`stg_games`
WHERE required_age < 0 OR required_age > 18