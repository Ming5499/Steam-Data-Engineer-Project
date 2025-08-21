
    
    

select
    dev_id as unique_field,
    count(*) as n_records

from `steam_db_staging`.`stg_developers`
where dev_id is not null
group by dev_id
having count(*) > 1


