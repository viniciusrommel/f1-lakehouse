SELECT season, round, driver_code, count(*) AS n
FROM {{ ref('mart_driver_race_result') }}
GROUP BY season, round, driver_code
HAVING count(*) > 1
