

SELECT
    pub_id,
    name
FROM `steam_db`.`publishers`
WHERE name IS NOT NULL AND name != ''