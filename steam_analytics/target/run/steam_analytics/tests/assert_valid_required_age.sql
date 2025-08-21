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
      SELECT *
FROM `steam_db_staging`.`stg_games`
WHERE required_age < 0 OR required_age > 18
      
    ) dbt_internal_test