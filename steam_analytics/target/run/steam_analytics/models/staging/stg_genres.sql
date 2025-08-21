
  create view `steam_db_staging`.`stg_genres__dbt_tmp`
    
    
  as (
    

SELECT
    genre_id,
    name
FROM `steam_db`.`genres`
WHERE name IS NOT NULL AND name != ''
  );