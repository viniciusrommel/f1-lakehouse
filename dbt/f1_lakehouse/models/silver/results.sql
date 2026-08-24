{{
  config(
    unique_key=['event_id'],
    post_hook="DELETE FROM {{ this }} WHERE _is_deleted = true"
  )
}}

WITH source AS (

    SELECT *
    FROM bronze.results

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
        driver_number,
        driver_code,
        full_name,
        team,
        team_color,

        CASE WHEN points >= 0 THEN points END AS points,
        position,
        classified_position,
        grid_position,

        q1_ms, q2_ms, q3_ms,
        race_time_ms,
        UPPER(TRIM(status)) AS status,

        -- Nao terminou a prova: classified_position vira letra ('R','D','W')
        classified_position IS NOT NULL
            AND NOT (classified_position RLIKE '^[0-9]+$')  AS is_dnf,

        COALESCE(__op, 'r') = 'd' AS _is_deleted,
        current_timestamp() AS _silver_loaded_at

    FROM current_state

)

SELECT
    *,
    q1_ms / 1000.0        AS q1_s,
    q2_ms / 1000.0        AS q2_s,
    q3_ms / 1000.0        AS q3_s,
    race_time_ms / 1000.0 AS race_time_s
FROM cleaned
