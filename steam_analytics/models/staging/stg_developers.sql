{{ config(materialized='view') }}

SELECT
    dev_id,
    name
FROM {{ source('steam_db', 'developers') }}
WHERE name IS NOT NULL AND name != ''