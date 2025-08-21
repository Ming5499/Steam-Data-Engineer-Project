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
      
    
    



select game_appid
from `steam_db_staging`.`stg_crawl_state`
where game_appid is null



      
    ) dbt_internal_test