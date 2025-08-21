{{ config(materialized='view') }}

SELECT
    pub_id,
    name
FROM {{ source('steam_db', 'publishers') }}
WHERE name IS NOT NULL AND name != ''