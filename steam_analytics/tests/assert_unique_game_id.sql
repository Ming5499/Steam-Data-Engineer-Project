SELECT game_id, COUNT(*) as count
FROM {{ ref('stg_games') }}
GROUP BY game_id
HAVING count > 1