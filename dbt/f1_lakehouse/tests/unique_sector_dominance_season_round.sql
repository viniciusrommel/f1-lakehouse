SELECT season, round, count(*) AS n
FROM {{ ref('sector_dominance') }}
GROUP BY season, round
HAVING count(*) > 1
