SELECT season, round, count(*) AS n
FROM {{ ref('race_weekend_summary') }}
GROUP BY season, round
HAVING count(*) > 1
