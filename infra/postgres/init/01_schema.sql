-- O Debezium le estas tabelas via WAL (replicacao logica), sem SELECT direto.

CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id        BIGSERIAL PRIMARY KEY,
    season          INT          NOT NULL,
    round           INT          NOT NULL,
    session_type    TEXT         NOT NULL,
    driver_number   TEXT         NOT NULL,
    driver_code     TEXT,
    team            TEXT,
    lap_number      INT,
    session_time_ms BIGINT       NOT NULL,
    speed_kph       REAL,
    throttle_pct    REAL,
    brake           BOOLEAN,
    gear            INT,
    rpm             REAL,
    drs             INT,
    x_pos           REAL,
    y_pos           REAL,
    z_pos           REAL,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lap_events (
    event_id         BIGSERIAL PRIMARY KEY,
    season           INT         NOT NULL,
    round            INT         NOT NULL,
    session_type     TEXT        NOT NULL,
    driver_number    TEXT        NOT NULL,
    driver_code      TEXT,
    team             TEXT,
    lap_number       INT         NOT NULL,
    lap_time_ms      BIGINT,
    sector1_ms       BIGINT,
    sector2_ms       BIGINT,
    sector3_ms       BIGINT,
    compound         TEXT,
    tyre_life        INT,
    is_personal_best BOOLEAN,
    pit_in_time_ms   BIGINT,
    pit_out_time_ms  BIGINT,
    track_status     TEXT,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS weather_events (
    event_id        BIGSERIAL PRIMARY KEY,
    season          INT         NOT NULL,
    round           INT         NOT NULL,
    session_type    TEXT        NOT NULL,
    session_time_ms BIGINT,
    air_temp_c      REAL,
    humidity_pct    REAL,
    pressure_mbar   REAL,
    rainfall        BOOLEAN,
    track_temp_c    REAL,
    wind_direction  INT,
    wind_speed_ms   REAL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS race_control_messages (
    event_id        BIGSERIAL PRIMARY KEY,
    season          INT         NOT NULL,
    round           INT         NOT NULL,
    session_type    TEXT        NOT NULL,
    msg_time        TIMESTAMPTZ,
    category        TEXT,
    message         TEXT,
    status          TEXT,
    flag            TEXT,
    scope           TEXT,
    sector          INT,
    racing_number   TEXT,
    lap_number      INT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS track_status_events (
    event_id        BIGSERIAL PRIMARY KEY,
    season          INT         NOT NULL,
    round           INT         NOT NULL,
    session_type    TEXT        NOT NULL,
    session_time_ms BIGINT,
    status_code     TEXT,
    message         TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS session_status_events (
    event_id        BIGSERIAL PRIMARY KEY,
    season          INT         NOT NULL,
    round           INT         NOT NULL,
    session_type    TEXT        NOT NULL,
    session_time_ms BIGINT,
    status          TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS session_results (
    event_id             BIGSERIAL PRIMARY KEY,
    season               INT         NOT NULL,
    round                INT         NOT NULL,
    session_type         TEXT        NOT NULL,
    driver_number        TEXT        NOT NULL,
    driver_code          TEXT,
    full_name            TEXT,
    team                 TEXT,
    team_color           TEXT,
    position             REAL,
    classified_position  TEXT,
    grid_position        REAL,
    q1_ms                BIGINT,
    q2_ms                BIGINT,
    q3_ms                BIGINT,
    race_time_ms         BIGINT,
    status               TEXT,
    points               REAL,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tel_session     ON telemetry_events       (season, round, session_type, driver_number);
CREATE INDEX IF NOT EXISTS idx_lap_session     ON lap_events             (season, round, session_type, driver_number);
CREATE INDEX IF NOT EXISTS idx_weather_session ON weather_events         (season, round, session_type);
CREATE INDEX IF NOT EXISTS idx_rcm_session     ON race_control_messages  (season, round, session_type);
CREATE INDEX IF NOT EXISTS idx_track_session   ON track_status_events    (season, round, session_type);
CREATE INDEX IF NOT EXISTS idx_sess_session    ON session_status_events  (season, round, session_type);
CREATE INDEX IF NOT EXISTS idx_results_session ON session_results        (season, round, session_type);

-- REPLICA IDENTITY FULL: faz o WAL incluir a linha antiga completa em
-- UPDATE/DELETE, para o Debezium emitir a imagem anterior inteira.
ALTER TABLE telemetry_events       REPLICA IDENTITY FULL;
ALTER TABLE lap_events             REPLICA IDENTITY FULL;
ALTER TABLE weather_events         REPLICA IDENTITY FULL;
ALTER TABLE race_control_messages  REPLICA IDENTITY FULL;
ALTER TABLE track_status_events    REPLICA IDENTITY FULL;
ALTER TABLE session_status_events  REPLICA IDENTITY FULL;
ALTER TABLE session_results        REPLICA IDENTITY FULL;

CREATE PUBLICATION f1_publication FOR TABLE
    telemetry_events, lap_events, weather_events, race_control_messages,
    track_status_events, session_status_events, session_results;
