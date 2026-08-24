"""Acesso ao Postgres pelo painel Streamlit: tabela de auditoria
`ingestion_jobs` e consultas de cobertura sobre as tabelas do CDC.

Usa SQLAlchemy Engine em vez de conexão psycopg2 crua: pandas não suporta
oficialmente read_sql sobre DBAPI2 puro e na prática travava o processo.
"""
from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_USER = os.environ.get("PG_USER", "f1user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "f1pass")
PG_DATABASE = os.environ.get("PG_DATABASE", "f1oltp")

# NullPool: sem reuso de conexão entre as threads do Streamlit. Conexão
# compartilhada entre threads nativas (psycopg2/pyarrow) causava segfault.
ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}",
    poolclass=NullPool,
)


def bootstrap() -> None:
    """Cria a tabela e a inclui no CDC. Idempotente.

    REPLICA IDENTITY FULL porque esta tabela sofre UPDATE (running -> done),
    e o Debezium precisa da imagem completa antes/depois.
    """
    with ENGINE.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ingestion_jobs (
                id               BIGSERIAL PRIMARY KEY,
                season           INT         NOT NULL,
                gp               TEXT        NOT NULL,
                session_type     TEXT        NOT NULL,
                driver           TEXT,                     -- NULL = todos os pilotos
                mode             TEXT        NOT NULL,      -- batch | replay
                speed            REAL,
                telemetry_limit  INT,
                status           TEXT        NOT NULL DEFAULT 'running',
                pid              INT,
                log_path         TEXT,
                error_message    TEXT,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at      TIMESTAMPTZ
            );
        """))
        conn.execute(text("ALTER TABLE ingestion_jobs REPLICA IDENTITY FULL;"))
        already_published = conn.execute(text("""
            SELECT 1 FROM pg_publication_tables
            WHERE pubname = 'f1_publication' AND tablename = 'ingestion_jobs'
        """)).fetchone()
        if already_published is None:
            conn.execute(text("ALTER PUBLICATION f1_publication ADD TABLE ingestion_jobs;"))


def insert_job(*, season: int, gp: str, session_type: str, driver: str | None,
                mode: str, speed: float | None, telemetry_limit: int | None,
                pid: int, log_path: str) -> int:
    with ENGINE.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO ingestion_jobs
                    (season, gp, session_type, driver, mode, speed, telemetry_limit, pid, log_path, status)
                VALUES (:season, :gp, :session_type, :driver, :mode, :speed, :telemetry_limit, :pid, :log_path, 'running')
                RETURNING id
            """),
            dict(season=int(season), gp=gp, session_type=session_type, driver=driver,
                 mode=mode, speed=speed, telemetry_limit=telemetry_limit,
                 pid=int(pid), log_path=log_path),
        )
        return result.scalar_one()


def mark_job_finished(job_id: int, *, success: bool, error_message: str | None = None) -> None:
    with ENGINE.begin() as conn:
        conn.execute(
            text("""
                UPDATE ingestion_jobs
                SET status = :status, finished_at = :finished_at, error_message = :error_message
                WHERE id = :job_id
            """),
            dict(status="done" if success else "failed", finished_at=datetime.utcnow(),
                 error_message=error_message, job_id=int(job_id)),
        )


def mark_job_cancelled(job_id: int) -> None:
    """Parada solicitada pelo usuário — status separado de 'failed'."""
    with ENGINE.begin() as conn:
        conn.execute(
            text("""
                UPDATE ingestion_jobs
                SET status = 'cancelled', finished_at = :finished_at
                WHERE id = :job_id AND status = 'running'
            """),
            dict(finished_at=datetime.utcnow(), job_id=int(job_id)),
        )


def mark_job_unknown(job_id: int) -> None:
    """Container reiniciou durante o job: handle perdido, status indefinido."""
    with ENGINE.begin() as conn:
        conn.execute(
            text("UPDATE ingestion_jobs SET status = 'unknown' WHERE id = :job_id AND status = 'running'"),
            dict(job_id=int(job_id)),
        )


def fetch_jobs(limit: int = 100) -> pd.DataFrame:
    df = pd.read_sql(
        text("""
            SELECT id, season, gp, session_type, COALESCE(driver, 'todos') AS driver,
                   mode, status, log_path, error_message, created_at, finished_at,
                   EXTRACT(EPOCH FROM (COALESCE(finished_at, now()) - created_at))::int AS duration_s
            FROM ingestion_jobs
            ORDER BY created_at DESC
            LIMIT :limit
        """),
        ENGINE, params={"limit": int(limit)},
    )
    # Converte para string: pyarrow tem bug (segfault) ao serializar coluna
    # datetime com timezone contendo NaT (job rodando tem finished_at NULL).
    for col in ("created_at", "finished_at"):
        df[col] = df[col].apply(lambda v: v.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(v) else "—")
    return df


def fetch_coverage() -> pd.DataFrame:
    """Matriz de cobertura: o que já está carregado nas tabelas do CDC."""
    return pd.read_sql(
        text("""
            SELECT
                t.season,
                t.round,
                t.session_type,
                count(DISTINCT t.driver_code)  AS pilotos,
                count(*)                        AS linhas_telemetria,
                COALESCE(l.laps, 0)             AS linhas_laps
            FROM telemetry_events t
            LEFT JOIN (
                SELECT season, round, session_type, count(*) AS laps
                FROM lap_events
                GROUP BY 1, 2, 3
            ) l USING (season, round, session_type)
            GROUP BY t.season, t.round, t.session_type, l.laps
            ORDER BY t.season DESC, t.round DESC, t.session_type
        """),
        ENGINE,
    )


def fetch_table_counts() -> pd.DataFrame:
    """Contagem de linhas por tabela capturada pelo CDC."""
    return pd.read_sql(
        text("""
            SELECT 'telemetry_events' AS tabela, count(*) AS linhas FROM telemetry_events
            UNION ALL SELECT 'lap_events', count(*) FROM lap_events
            UNION ALL SELECT 'weather_events', count(*) FROM weather_events
            UNION ALL SELECT 'race_control_messages', count(*) FROM race_control_messages
            UNION ALL SELECT 'track_status_events', count(*) FROM track_status_events
            UNION ALL SELECT 'session_status_events', count(*) FROM session_status_events
            UNION ALL SELECT 'session_results', count(*) FROM session_results
            UNION ALL SELECT 'ingestion_jobs', count(*) FROM ingestion_jobs
            ORDER BY tabela
        """),
        ENGINE,
    )


def fetch_results_for(season: int, round_: int, session_type: str) -> pd.DataFrame:
    return pd.read_sql(
        text("""
            SELECT position, driver_code, full_name, team, points, status
            FROM session_results
            WHERE season = :season AND round = :round_ AND session_type = :session_type
            ORDER BY position
        """),
        ENGINE, params={"season": int(season), "round_": int(round_), "session_type": session_type},
    )


def fetch_drivers_for(season: int, round_: int, session_type: str) -> pd.DataFrame:
    return pd.read_sql(
        text("""
            SELECT DISTINCT driver_code, team, count(*) AS linhas
            FROM telemetry_events
            WHERE season = :season AND round = :round_ AND session_type = :session_type
            GROUP BY driver_code, team
            ORDER BY driver_code
        """),
        ENGINE, params={"season": int(season), "round_": int(round_), "session_type": session_type},
    )
