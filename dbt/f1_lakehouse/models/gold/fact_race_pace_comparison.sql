{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['season', 'round', 'driver_code'],
    partition_by=['season', 'round'],
    file_format='delta'
  )
}}

SELECT
    season,
    round,
    driver_code,
    MAX(team)           AS team,
    COUNT(*)            AS race_laps,
    MIN(lap_time_s)     AS best_lap_s,
    AVG(lap_time_s)     AS avg_lap_s,
    AVG(sector1_s)      AS avg_s1,
    AVG(sector2_s)      AS avg_s2,
    AVG(sector3_s)      AS avg_s3,
    current_timestamp() AS _gold_loaded_at

FROM {{ ref('laps') }}
WHERE lap_time_ms IS NOT NULL
  AND session_type = 'R'

{% if is_incremental() %}
  -- So as corridas cuja Silver mudou desde o ultimo build
  AND concat(season, '-', round) IN (
      SELECT DISTINCT concat(season, '-', round)
      FROM {{ ref('laps') }}
      WHERE _silver_loaded_at > (
          SELECT COALESCE(MAX(_gold_loaded_at), TIMESTAMP '1970-01-01 00:00:00')
          FROM {{ this }}
      )
  )
{% endif %}

GROUP BY season, round, driver_code
