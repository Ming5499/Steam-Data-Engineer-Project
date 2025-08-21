SELECT *
FROM {{ ref('stg_games') }}
WHERE game_id IS NULL OR title IS NULL