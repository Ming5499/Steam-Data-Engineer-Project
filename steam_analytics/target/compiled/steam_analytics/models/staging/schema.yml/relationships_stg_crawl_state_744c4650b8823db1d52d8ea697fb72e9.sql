
    
    

with child as (
    select game_appid as from_field
    from `steam_db_staging`.`stg_crawl_state`
    where game_appid is not null
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


