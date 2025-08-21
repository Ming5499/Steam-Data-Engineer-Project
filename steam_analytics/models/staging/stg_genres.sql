{{ config(materialized='view') }}

SELECT
    genre_id,
    name
FROM {{ source('steam_db', 'genres') }}
WHERE name IS NOT NULL AND name != ''