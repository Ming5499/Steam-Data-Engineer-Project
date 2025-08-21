

SELECT
    price_id,
    game_id,
    price,
    discount,
    initial_price,
    timestamp
FROM `steam_db`.`prices`
WHERE price >= 0 AND discount >= 0 AND initial_price >= 0