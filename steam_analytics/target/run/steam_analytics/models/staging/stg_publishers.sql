
  create view `steam_db_staging`.`stg_publishers__dbt_tmp`
    
    
  as (
    

SELECT
    pub_id,
    name
FROM `steam_db`.`publishers`
WHERE name IS NOT NULL AND name != ''
  );