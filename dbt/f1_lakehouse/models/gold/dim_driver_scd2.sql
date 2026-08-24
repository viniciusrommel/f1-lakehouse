-- Nao usa dbt snapshot, que so captura mudancas dali pra frente: o
-- historico ja existe inteiro na silver.results, entao as versoes sao
-- derivadas por soma cumulativa de flags de mudanca ("islands").
-- Como a origem nao tem data, a ordem e race_seq = season*100 + round.
{{ config(materialized='table', file_format='delta') }}

WITH per_race AS (

    SELECT DISTINCT
        driver_code,
        full_name,
        team,
        team_color,
        season * 100 + round AS race_seq
    FROM {{ ref('results') }}
    WHERE driver_code IS NOT NULL

),

with_change_flag AS (

    SELECT
        *,
        CASE
            WHEN LAG(full_name)  OVER (PARTITION BY driver_code ORDER BY race_seq) IS DISTINCT FROM full_name
              OR LAG(team)       OVER (PARTITION BY driver_code ORDER BY race_seq) IS DISTINCT FROM team
              OR LAG(team_color) OVER (PARTITION BY driver_code ORDER BY race_seq) IS DISTINCT FROM team_color
            THEN 1 ELSE 0
        END AS is_change
    FROM per_race

),

grouped AS (

    SELECT
        *,
        SUM(is_change) OVER (PARTITION BY driver_code ORDER BY race_seq) AS version_group
    FROM with_change_flag

),

versions AS (

    SELECT
        driver_code,
        full_name,
        team,
        team_color,
        MIN(race_seq) AS _valid_from_seq,
        MAX(race_seq) AS _valid_to_seq_observed
    FROM grouped
    GROUP BY driver_code, full_name, team, team_color, version_group

)

SELECT
    driver_code,
    full_name,
    team,
    team_color,

    CAST(_valid_from_seq DIV 100 AS INT)        AS valid_from_season,
    CAST(_valid_from_seq % 100 AS INT)          AS valid_from_round,
    CAST(_valid_to_seq_observed DIV 100 AS INT) AS valid_to_season,
    CAST(_valid_to_seq_observed % 100 AS INT)   AS valid_to_round,
    LEAD(_valid_from_seq) OVER (PARTITION BY driver_code ORDER BY _valid_from_seq) IS NULL AS is_current,

    -- Auxiliares para join point-in-time: estende o limite superior ate o
    -- inicio da versao seguinte. 999999 marca a versao ainda aberta.
    _valid_from_seq,
    COALESCE(
        LEAD(_valid_from_seq) OVER (PARTITION BY driver_code ORDER BY _valid_from_seq) - 1,
        999999
    ) AS _valid_to_seq_excl,

    current_timestamp() AS _gold_loaded_at

FROM versions
