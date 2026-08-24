{{
  config(
    unique_key=['season', 'round', 'session_type', 'driver_number', 'lap_number'],
    post_hook="DELETE FROM {{ this }} WHERE _is_deleted = true"
  )
}}

-- Reconstroi o estado atual a partir do log CDC da Bronze: deduplica pela
-- chave natural ordenando por __source_lsn (ordem total do WAL).
--
-- Deletes entram no merge como tombstone (_is_deleted) e o post_hook os
-- remove logo depois. Filtrar deletes ANTES do merge so funciona em full
-- refresh: no incremental, uma chave cujo unico evento do lote e um delete
-- nunca chegaria ao merge, e a linha ficaria presa na Silver.
WITH source AS (

    SELECT *
    FROM bronze.laps

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
            PARTITION BY season, round, session_type, driver_number, lap_number
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

        -- Descarta tempos absurdos: < 60 s ou > 60 min
        CASE WHEN lap_time_ms BETWEEN 60000 AND 3600000
             THEN lap_time_ms END                            AS lap_time_ms,

        sector1_ms,
        sector2_ms,
        sector3_ms,

        CASE
            WHEN UPPER(compound) IN (
                'SOFT','MEDIUM','HARD','INTERMEDIATE','WET',
                'SUPERSOFT','ULTRASOFT','HYPERSOFT','SUPERHARD'
            ) THEN UPPER(compound)
            ELSE 'UNKNOWN'
        END                                                  AS compound,

        tyre_life,
        is_personal_best,
        pit_in_time_ms,
        pit_out_time_ms,
        track_status,

        COALESCE(__op, 'r') = 'd'                            AS _is_deleted,
        current_timestamp()                                  AS _silver_loaded_at

    FROM current_state

),

with_derived AS (

    SELECT
        *,
        lap_time_ms / 1000.0  AS lap_time_s,
        sector1_ms  / 1000.0  AS sector1_s,
        sector2_ms  / 1000.0  AS sector2_s,
        sector3_ms  / 1000.0  AS sector3_s,

        MIN(lap_time_ms) OVER (
            PARTITION BY season, round, session_type
        ) = lap_time_ms       AS is_fastest_lap

    FROM cleaned

)

SELECT * FROM with_derived
