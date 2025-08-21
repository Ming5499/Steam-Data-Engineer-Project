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
      
    
    

select
    dev_id as unique_field,
    count(*) as n_records

from `steam_db_staging`.`stg_developers`
where dev_id is not null
group by dev_id
having count(*) > 1



      
    ) dbt_internal_test