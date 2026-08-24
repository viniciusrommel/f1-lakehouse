{{
  config(
    unique_key=['event_id'],
    post_hook="DELETE FROM {{ this }} WHERE _is_deleted = true"
  )
}}

WITH source AS (

    SELECT *
    FROM bronze.weather

    {% if is_incremental() %}
    WHERE _bronze_loaded_at > (
        SELECT COALESCE(MAX(_silver_loaded_at), TIMESTAMP '1970-01-01 00:00:00')
        FROM {{ this }}
    )
    {% endif %}

),

deduped AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY __source_lsn DESC NULLS LAST, __ts_ms DESC NULLS LAST
        ) AS _rn
    FROM source

),

current_state AS (

    SELECT * FROM deduped
    WHERE _rn = 1

),

cleaned AS (

    SELECT
        event_id,
        season,
        round,
        session_type,
        session_time_ms,

        CASE WHEN air_temp_c   BETWEEN -10 AND 60 THEN air_temp_c   END AS air_temp_c,
        CASE WHEN track_temp_c BETWEEN -10 AND 90 THEN track_temp_c END AS track_temp_c,
        CASE WHEN humidity_pct BETWEEN 0 AND 100   THEN humidity_pct END AS humidity_pct,
        CASE WHEN pressure_mbar BETWEEN 800 AND 1100 THEN pressure_mbar END AS pressure_mbar,
        CASE WHEN wind_speed_ms >= 0 THEN wind_speed_ms END AS wind_speed_ms,
        wind_direction,
        COALESCE(rainfall, false) AS rainfall,

        COALESCE(__op, 'r') = 'd' AS _is_deleted,
        current_timestamp() AS _silver_loaded_at

    FROM current_state

)

SELECT
    *,
    session_time_ms / 1000.0 AS session_time_s
FROM cleaned
