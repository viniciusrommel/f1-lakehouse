SELECT driver_code, valid_from_season, valid_from_round, count(*) AS n
FROM {{ ref('dim_driver_scd2') }}
GROUP BY driver_code, valid_from_season, valid_from_round
HAVING count(*) > 1
