
    
    

select
    pub_id as unique_field,
    count(*) as n_records

from `steam_db_staging`.`stg_publishers`
where pub_id is not null
group by pub_id
having count(*) > 1


