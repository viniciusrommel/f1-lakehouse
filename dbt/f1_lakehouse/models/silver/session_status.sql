{{
  config(
    unique_key=['event_id'],
    post_hook="DELETE FROM {{ this }} WHERE _is_deleted = true"
  )
}}

WITH source AS (

    SELECT *
    FROM bronze.session_status

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
    session_time_ms,
    session_time_ms / 1000.0 AS session_time_s,
    UPPER(TRIM(status))      AS status,
    COALESCE(__op, 'r') = 'd' AS _is_deleted,
    current_timestamp()      AS _silver_loaded_at

FROM current_state
