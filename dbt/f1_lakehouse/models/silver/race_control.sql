{{
  config(
    unique_key=['event_id'],
    post_hook="DELETE FROM {{ this }} WHERE _is_deleted = true"
  )
}}

WITH source AS (

    SELECT *
    FROM bronze.race_control

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

)

SELECT
    event_id,
    season,
    round,
    session_type,
    msg_time,

    UPPER(TRIM(category))      AS category,
    message,
    UPPER(TRIM(status))        AS status,
    UPPER(TRIM(flag))          AS flag,
    UPPER(TRIM(scope))         AS scope,
    sector,
    racing_number,
    lap_number,

    UPPER(TRIM(flag)) IN ('YELLOW', 'DOUBLE YELLOW')      AS is_yellow_flag,
    UPPER(TRIM(category)) = 'SAFETYCAR'                    AS is_safety_car,
    UPPER(TRIM(message)) LIKE '%DRS ENABLED%'              AS is_drs_enabled_msg,
    UPPER(TRIM(message)) LIKE '%DRS DISABLED%'             AS is_drs_disabled_msg,

    COALESCE(__op, 'r') = 'd' AS _is_deleted,
    current_timestamp() AS _silver_loaded_at

FROM current_state
