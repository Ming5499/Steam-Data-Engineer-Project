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
      SELECT game_id, COUNT(*) as count
FROM `steam_db_staging`.`stg_games`
GROUP BY game_id
HAVING count > 1
      
    ) dbt_internal_test