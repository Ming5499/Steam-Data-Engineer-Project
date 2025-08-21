

SELECT
    dev_id,
    name
FROM `steam_db`.`developers`
WHERE name IS NOT NULL AND name != ''