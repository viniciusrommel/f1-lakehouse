SELECT team, valid_from_season, valid_from_round, count(*) AS n
FROM {{ ref('dim_team_scd2') }}
GROUP BY team, valid_from_season, valid_from_round
HAVING count(*) > 1
