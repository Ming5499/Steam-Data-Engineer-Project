
    
    

select
    game_appid as unique_field,
    count(*) as n_records

from `steam_db_staging`.`stg_crawl_state`
where game_appid is not null
group by game_appid
having count(*) > 1


