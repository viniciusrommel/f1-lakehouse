{{
  config(
    unique_key=['season', 'round', 'session_type', 'driver_number', 'session_time_ms'],
    post_hook="DELETE FROM {{ this }} WHERE _is_deleted = true"
  )
}}

WITH source AS (

    SELECT *
    FROM bronze.telemetry

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
            PARTITION BY season, round, session_type, driver_number, session_time_ms
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
        driver_number,
        driver_code,
        team,
        lap_number,
        session_time_ms,

        CASE WHEN speed_kph    BETWEEN 0 AND 400   THEN speed_kph    END AS speed_kph,
        CASE WHEN throttle_pct BETWEEN 0 AND 100   THEN throttle_pct END AS throttle_pct,
        brake,
        CASE WHEN gear         BETWEEN 0 AND 8     THEN gear         END AS gear,
        CASE WHEN rpm          BETWEEN 0 AND 20000 THEN rpm          END AS rpm,

        -- DRS: os valores 8/10/12/14 significam ativo
        drs IN (8, 10, 12, 14)  AS drs_active,

        x_pos,
        y_pos,
        z_pos,

        COALESCE(__op, 'r') = 'd' AS _is_deleted,
        current_timestamp()     AS _silver_loaded_at

    FROM current_state

)

SELECT * FROM cleaned
