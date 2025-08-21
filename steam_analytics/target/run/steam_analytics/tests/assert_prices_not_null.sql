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
FROM `steam_db_staging`.`stg_prices`
WHERE price_id IS NULL OR game_id IS NULL OR price IS NULL OR discount IS NULL OR initial_price IS NULL
      
    ) dbt_internal_test