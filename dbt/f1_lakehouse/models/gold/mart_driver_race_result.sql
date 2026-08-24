{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['season', 'round', 'driver_code'],
    partition_by=['season', 'round'],
    file_format='delta'
  )
}}

WITH races AS (

    SELECT
        season,
        round,
        season * 100 + round AS race_seq,
        driver_code,
        team,
        grid_position,
        position,
        classified_position,
        points,
        is_dnf,
        race_time_s
    FROM {{ ref('results') }}
    WHERE session_type = 'R'

    {% if is_incremental() %}
    AND concat(season, '-', round) IN (
        SELECT DISTINCT concat(season, '-', round)
        FROM {{ ref('results') }}
        WHERE _silver_loaded_at > (
            SELECT COALESCE(MAX(_gold_loaded_at), TIMESTAMP '1970-01-01 00:00:00')
            FROM {{ this }}
        )
    )
    {% endif %}

),

lap_agg AS (

    SELECT
        season,
        round,
        driver_code,
        MIN(lap_time_s)                      AS fastest_lap_s,
        COUNT_IF(pit_in_time_ms IS NOT NULL) AS pit_stops
    FROM {{ ref('laps') }}
    WHERE session_type = 'R'
    GROUP BY season, round, driver_code

),

quali AS (

    SELECT season, round, driver_code, q1_s, q2_s, q3_s
    FROM {{ ref('results') }}
    WHERE session_type = 'Q'

)

SELECT
    r.season,
    r.round,
    r.driver_code,
    dd.full_name         AS driver_full_name,     -- valor vigente na corrida
    r.team,
    dt.team_color,                                -- valor vigente na corrida
    r.grid_position,
    r.position,
    r.classified_position,
    r.points,
    r.is_dnf,
    r.race_time_s,
    l.fastest_lap_s,
    l.pit_stops,
    q.q1_s,
    q.q2_s,
    q.q3_s,
    rp.best_lap_s         AS race_best_lap_s,
    rp.avg_lap_s          AS race_avg_lap_s,
    rp.avg_s1             AS race_avg_s1,
    rp.avg_s2             AS race_avg_s2,
    rp.avg_s3             AS race_avg_s3,
    ds.avg_lap_s          AS season_avg_lap_s,
    ds.lap_consistency_stddev AS season_lap_consistency_stddev,
    ds.personal_bests     AS season_personal_bests,
    current_timestamp() AS _gold_loaded_at

FROM races r
LEFT JOIN lap_agg l
    ON r.season = l.season AND r.round = l.round AND r.driver_code = l.driver_code
LEFT JOIN quali q
    ON r.season = q.season AND r.round = q.round AND r.driver_code = q.driver_code
LEFT JOIN {{ ref('dim_driver_scd2') }} dd
    ON r.driver_code = dd.driver_code
   AND r.race_seq BETWEEN dd._valid_from_seq AND dd._valid_to_seq_excl
LEFT JOIN {{ ref('dim_team_scd2') }} dt
    ON r.team = dt.team
   AND r.race_seq BETWEEN dt._valid_from_seq AND dt._valid_to_seq_excl
LEFT JOIN {{ ref('fact_race_pace_comparison') }} rp
    ON r.season = rp.season AND r.round = rp.round AND r.driver_code = rp.driver_code
LEFT JOIN {{ ref('fact_driver_season_summary') }} ds
    ON r.season = ds.season AND r.driver_code = ds.driver_code
