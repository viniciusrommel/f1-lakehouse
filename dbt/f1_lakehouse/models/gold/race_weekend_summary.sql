{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['season', 'round'],
    partition_by=['season', 'round'],
    file_format='delta'
  )
}}

{% set race_filter = "session_type = 'R'" %}

WITH results_agg AS (

    SELECT
        season,
        round,
        MAX(CASE WHEN position = 1 THEN driver_code END) AS winner_code,
        MAX(CASE WHEN position = 1 THEN full_name   END) AS winner_name,
        MAX(CASE WHEN position = 1 THEN team        END) AS winner_team,
        MAX(CASE WHEN grid_position = 1 THEN driver_code END) AS pole_code,
        COUNT(*)                        AS drivers_classified,
        COUNT_IF(is_dnf)                AS dnf_count
    FROM {{ ref('results') }}
    WHERE {{ race_filter }}
    GROUP BY season, round

),

laps_agg AS (

    SELECT
        season,
        round,
        COUNT(*)          AS total_race_laps,
        MIN(lap_time_s)   AS fastest_lap_s,
        AVG(lap_time_s)   AS avg_lap_s
    FROM {{ ref('laps') }}
    WHERE {{ race_filter }}
      AND lap_time_ms IS NOT NULL
    GROUP BY season, round

),

weather_agg AS (

    SELECT
        season,
        round,
        ROUND(AVG(air_temp_c), 1)   AS avg_air_temp_c,
        ROUND(AVG(track_temp_c), 1) AS avg_track_temp_c,
        ROUND(MAX(humidity_pct), 1) AS max_humidity_pct,
        MAX(rainfall)               AS was_wet_race
    FROM {{ ref('weather') }}
    WHERE {{ race_filter }}
    GROUP BY season, round

),

race_control_agg AS (

    SELECT
        season,
        round,
        COUNT_IF(is_safety_car)                 AS safety_car_msgs,
        COUNT_IF(is_yellow_flag)                AS yellow_flag_msgs,
        COUNT_IF(flag = 'RED')                  AS red_flag_msgs
    FROM {{ ref('race_control') }}
    WHERE {{ race_filter }}
    GROUP BY season, round

)

SELECT
    r.season,
    r.round,
    r.winner_code,
    r.winner_name,
    r.winner_team,
    r.pole_code,
    r.drivers_classified,
    r.dnf_count,
    l.total_race_laps,
    l.fastest_lap_s,
    l.avg_lap_s,
    w.avg_air_temp_c,
    w.avg_track_temp_c,
    w.max_humidity_pct,
    COALESCE(w.was_wet_race, false) AS was_wet_race,
    COALESCE(rc.safety_car_msgs, 0)  AS safety_car_msgs,
    COALESCE(rc.yellow_flag_msgs, 0) AS yellow_flag_msgs,
    COALESCE(rc.red_flag_msgs, 0)    AS red_flag_msgs,

    current_timestamp() AS _gold_loaded_at

FROM results_agg r
LEFT JOIN laps_agg          l  ON r.season = l.season  AND r.round = l.round
LEFT JOIN weather_agg       w  ON r.season = w.season  AND r.round = w.round
LEFT JOIN race_control_agg  rc ON r.season = rc.season AND r.round = rc.round

{% if is_incremental() %}
-- So as corridas cuja Silver de origem mudou desde o ultimo build
WHERE concat(r.season, '-', r.round) IN (
    SELECT DISTINCT concat(season, '-', round) FROM {{ ref('results') }}
      WHERE _silver_loaded_at > (SELECT COALESCE(MAX(_gold_loaded_at), TIMESTAMP '1970-01-01 00:00:00') FROM {{ this }})
    UNION
    SELECT DISTINCT concat(season, '-', round) FROM {{ ref('laps') }}
      WHERE _silver_loaded_at > (SELECT COALESCE(MAX(_gold_loaded_at), TIMESTAMP '1970-01-01 00:00:00') FROM {{ this }})
    UNION
    SELECT DISTINCT concat(season, '-', round) FROM {{ ref('weather') }}
      WHERE _silver_loaded_at > (SELECT COALESCE(MAX(_gold_loaded_at), TIMESTAMP '1970-01-01 00:00:00') FROM {{ this }})
    UNION
    SELECT DISTINCT concat(season, '-', round) FROM {{ ref('race_control') }}
      WHERE _silver_loaded_at > (SELECT COALESCE(MAX(_gold_loaded_at), TIMESTAMP '1970-01-01 00:00:00') FROM {{ this }})
)
{% endif %}
