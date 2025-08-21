
  create view `steam_db_staging`.`stg_developers__dbt_tmp`
    
    
  as (
    

SELECT
    dev_id,
    name
FROM `steam_db`.`developers`
WHERE name IS NOT NULL AND name != ''
  );