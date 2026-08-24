-- Chave natural e o nome da equipe: a origem nao tem team_id estavel,
-- entao uma troca de nome aparece como equipe nova.
{{ config(materialized='table', file_format='delta') }}

WITH per_race AS (

    SELECT DISTINCT
        team,
        team_color,
        season * 100 + round AS race_seq
    FROM {{ ref('results') }}
    WHERE team IS NOT NULL

),

with_change_flag AS (

    SELECT
        *,
        CASE
            WHEN LAG(team_color) OVER (PARTITION BY team ORDER BY race_seq) IS DISTINCT FROM team_color
            THEN 1 ELSE 0
        END AS is_change
    FROM per_race

),

grouped AS (

    SELECT
        *,
        SUM(is_change) OVER (PARTITION BY team ORDER BY race_seq) AS version_group
    FROM with_change_flag

),

versions AS (

    SELECT
        team,
        team_color,
        MIN(race_seq) AS _valid_from_seq,
        MAX(race_seq) AS _valid_to_seq_observed
    FROM grouped
    GROUP BY team, team_color, version_group

)

SELECT
    team,
    team_color,

    CAST(_valid_from_seq DIV 100 AS INT)        AS valid_from_season,
    CAST(_valid_from_seq % 100 AS INT)          AS valid_from_round,
    CAST(_valid_to_seq_observed DIV 100 AS INT) AS valid_to_season,
    CAST(_valid_to_seq_observed % 100 AS INT)   AS valid_to_round,
    LEAD(_valid_from_seq) OVER (PARTITION BY team ORDER BY _valid_from_seq) IS NULL AS is_current,

    _valid_from_seq,
    COALESCE(
        LEAD(_valid_from_seq) OVER (PARTITION BY team ORDER BY _valid_from_seq) - 1,
        999999
    ) AS _valid_to_seq_excl,

    current_timestamp() AS _gold_loaded_at

FROM versions
