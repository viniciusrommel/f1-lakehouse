{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['season', 'round'],
    partition_by=['season', 'round'],
    file_format='delta'
  )
}}

WITH sector_avgs AS (

    SELECT
        season,
        round,
        driver_code,
        MAX(team)        AS team,
        AVG(sector1_s)   AS avg_s1,
        AVG(sector2_s)   AS avg_s2,
        AVG(sector3_s)   AS avg_s3
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

),

ranked AS (

    SELECT
        *,
        RANK() OVER (PARTITION BY season, round ORDER BY avg_s1) AS s1_rank,
        RANK() OVER (PARTITION BY season, round ORDER BY avg_s2) AS s2_rank,
        RANK() OVER (PARTITION BY season, round ORDER BY avg_s3) AS s3_rank
    FROM sector_avgs

)

SELECT
    season,
    round,
    MAX(CASE WHEN s1_rank = 1 THEN driver_code END) AS fastest_s1_driver,
    MAX(CASE WHEN s1_rank = 1 THEN team        END) AS fastest_s1_team,
    MIN(CASE WHEN s1_rank = 1 THEN avg_s1      END) AS fastest_s1_s,
    MAX(CASE WHEN s2_rank = 1 THEN driver_code END) AS fastest_s2_driver,
    MAX(CASE WHEN s2_rank = 1 THEN team        END) AS fastest_s2_team,
    MIN(CASE WHEN s2_rank = 1 THEN avg_s2      END) AS fastest_s2_s,
    MAX(CASE WHEN s3_rank = 1 THEN driver_code END) AS fastest_s3_driver,
    MAX(CASE WHEN s3_rank = 1 THEN team        END) AS fastest_s3_team,
    MIN(CASE WHEN s3_rank = 1 THEN avg_s3      END) AS fastest_s3_s,
    current_timestamp()                              AS _gold_loaded_at

FROM ranked
GROUP BY season, round
