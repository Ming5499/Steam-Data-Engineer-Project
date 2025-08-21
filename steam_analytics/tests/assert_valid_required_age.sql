SELECT *
FROM {{ ref('stg_games') }}
WHERE required_age < 0 OR required_age > 18