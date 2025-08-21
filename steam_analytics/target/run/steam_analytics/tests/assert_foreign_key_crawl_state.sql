select
      count(*) as failures,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_warn,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_error
    from (
      SELECT cs.game_appid
FROM `steam_db_staging`.`stg_crawl_state` cs
LEFT JOIN `steam_db_staging`.`stg_games` g ON cs.game_appid = g.game_id
WHERE g.game_id IS NULL
      
    ) dbt_internal_test