
    
    

select
    lang_id as unique_field,
    count(*) as n_records

from `steam_db_staging`.`stg_languages`
where lang_id is not null
group by lang_id
having count(*) > 1


