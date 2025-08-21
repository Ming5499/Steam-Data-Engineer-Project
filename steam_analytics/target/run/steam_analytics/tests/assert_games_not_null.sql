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
WHERE game_id IS NULL OR title IS NULL
      
    ) dbt_internal_test