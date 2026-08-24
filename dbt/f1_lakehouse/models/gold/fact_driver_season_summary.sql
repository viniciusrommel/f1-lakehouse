{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['season', 'driver_code'],
    partition_by=['season'],
    file_format='delta'
  )
}}

SELECT
    season,
    driver_code,
    MAX(team)                       AS team,
    COUNT(*)                        AS total_laps,
    MIN(lap_time_s)                 AS fastest_lap_s,
    AVG(lap_time_s)                 AS avg_lap_s,
    STDDEV(lap_time_s)              AS lap_consistency_stddev,
    COUNT_IF(is_fastest_lap)        AS personal_bests,
    COUNT_IF(compound = 'SOFT')     AS soft_laps,
    COUNT_IF(compound = 'MEDIUM')   AS medium_laps,
    COUNT_IF(compound = 'HARD')     AS hard_laps,
    current_timestamp()             AS _gold_loaded_at

FROM {{ ref('laps') }}
WHERE lap_time_ms IS NOT NULL

{% if is_incremental() %}
  -- So as temporadas cuja Silver mudou desde o ultimo build
  AND season IN (
      SELECT DISTINCT season
      FROM {{ ref('laps') }}
      WHERE _silver_loaded_at > (
          SELECT COALESCE(MAX(_gold_loaded_at), TIMESTAMP '1970-01-01 00:00:00')
          FROM {{ this }}
      )
  )
{% endif %}

GROUP BY season, driver_code
