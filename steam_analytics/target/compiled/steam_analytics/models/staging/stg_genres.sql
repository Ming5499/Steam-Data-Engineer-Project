

SELECT
    genre_id,
    name
FROM `steam_db`.`genres`
WHERE name IS NOT NULL AND name != ''