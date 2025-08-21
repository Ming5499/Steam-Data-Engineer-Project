
  create view `steam_db_staging`.`stg_languages__dbt_tmp`
    
    
  as (
    

SELECT
    lang_id,
    name
FROM `steam_db`.`languages`
WHERE name IS NOT NULL AND name != ''
  );