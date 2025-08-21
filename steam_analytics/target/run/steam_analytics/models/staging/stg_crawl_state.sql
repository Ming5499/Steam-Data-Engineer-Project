
  create view `steam_db_staging`.`stg_crawl_state__dbt_tmp`
    
    
  as (
    

SELECT
    game_appid,
    last_review_timestamp,
    last_price_timestamp
FROM `steam_db`.`crawl_state`
  );