{{ config(materialized='table', persist_docs={'relation': true, 'columns': false}) }}

SELECT
    g.game_id,
    g.title,
    g.description,
    g.release_date,
    g.required_age,
    GROUP_CONCAT(DISTINCT d.name) AS developers,
    GROUP_CONCAT(DISTINCT p.name) AS publishers,
    GROUP_CONCAT(DISTINCT gn.name) AS genres,
    GROUP_CONCAT(DISTINCT l.name) AS languages
FROM {{ ref('stg_games') }} g
LEFT JOIN {{ source('steam_db', 'game_developers') }} gd ON g.game_id = gd.game_id
LEFT JOIN {{ ref('stg_developers') }} d ON gd.dev_id = d.dev_id
LEFT JOIN {{ source('steam_db', 'game_publishers') }} gp ON g.game_id = gp.game_id
LEFT JOIN {{ ref('stg_publishers') }} p ON gp.pub_id = p.pub_id
LEFT JOIN {{ source('steam_db', 'game_genres') }} gg ON g.game_id = gg.game_id
LEFT JOIN {{ ref('stg_genres') }} gn ON gg.genre_id = gn.genre_id
LEFT JOIN {{ source('steam_db', 'game_languages') }} gl ON g.game_id = gl.game_id
LEFT JOIN {{ ref('stg_languages') }} l ON gl.lang_id = l.lang_id
GROUP BY g.game_id, g.title, g.description, g.release_date, g.required_age