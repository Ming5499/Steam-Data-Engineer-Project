

SELECT
    game_id,
    title,
    description,
    release_date,
    windows_req,
    mac_req,
    linux_req,
    required_age,
    awards
FROM `steam_db`.`games`
WHERE title IS NOT NULL AND title != ''