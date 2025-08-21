
  create view `steam_db_staging`.`stg_games__dbt_tmp`
    
    
  as (
    

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
  );