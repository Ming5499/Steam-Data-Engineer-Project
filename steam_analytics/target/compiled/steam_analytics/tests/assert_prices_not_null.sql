SELECT *
FROM `steam_db_staging`.`stg_prices`
WHERE price_id IS NULL OR game_id IS NULL OR price IS NULL OR discount IS NULL OR initial_price IS NULL