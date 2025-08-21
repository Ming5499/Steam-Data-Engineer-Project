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
      
    
    



select dev_id
from `steam_db_staging`.`stg_developers`
where dev_id is null



      
    ) dbt_internal_test