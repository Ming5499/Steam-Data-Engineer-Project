
    
    

with child as (
    select game_id as from_field
    from `steam_db_staging`.`stg_prices`
    where game_id is not null
),

parent as (
    select game_id as to_field
    from `steam_db_staging`.`stg_games`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


