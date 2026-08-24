"""
FastF1 -> Postgres (OLTP). O Debezium captura os inserts pelo WAL.

Carrega 7 datasets por sessão. Só telemetria e voltas aceitam filtro de
piloto (--driver) e limite (--limit); os outros cinco são metadados da
sessão inteira.

Modos:
  --mode batch    insere tudo de uma vez (carga histórica)
  --mode replay   telemetria e voltas no ritmo real da sessão, numa linha do
                  tempo única (--speed comprime o tempo). Os datasets de
                  sessão continuam em lote no fim.

Uso:
  python -m ingestion.producers.fastf1_to_postgres --gp Bahrain --session R --driver VER --limit 200
  python -m ingestion.producers.fastf1_to_postgres --gp Bahrain --session R --mode replay --speed 20
"""
from __future__ import annotations

import argparse
import contextlib
import time

import fastf1
import pandas as pd
import psycopg2
import structlog
from psycopg2.extras import execute_values

from ingestion.config.settings import settings


def _i(v): return int(v) if pd.notna(v) else None
def _f(v): return float(v) if pd.notna(v) else None
def _b(v): return bool(v) if pd.notna(v) else None
def _s(v): return str(v) if pd.notna(v) else None
def _ms(td): return int(td.total_seconds() * 1000) if pd.notna(td) and hasattr(td, "total_seconds") else None
def _dt(ts): return ts.to_pydatetime() if pd.notna(ts) and hasattr(ts, "to_pydatetime") else None

log = structlog.get_logger()
fastf1.Cache.enable_cache(settings.fastf1_cache_dir)

TEL_COLS = [
    "season", "round", "session_type", "driver_number", "driver_code", "team",
    "lap_number", "session_time_ms", "speed_kph", "throttle_pct", "brake",
    "gear", "rpm", "drs", "x_pos", "y_pos", "z_pos",
]
LAP_COLS = [
    "season", "round", "session_type", "driver_number", "driver_code", "team",
    "lap_number", "lap_time_ms", "sector1_ms", "sector2_ms", "sector3_ms",
    "compound", "tyre_life", "is_personal_best", "pit_in_time_ms",
    "pit_out_time_ms", "track_status",
]
WEATHER_COLS = [
    "season", "round", "session_type", "session_time_ms",
    "air_temp_c", "humidity_pct", "pressure_mbar", "rainfall",
    "track_temp_c", "wind_direction", "wind_speed_ms",
]
RCM_COLS = [
    "season", "round", "session_type", "msg_time", "category", "message",
    "status", "flag", "scope", "sector", "racing_number", "lap_number",
]
TRACK_STATUS_COLS = [
    "season", "round", "session_type", "session_time_ms", "status_code", "message",
]
SESSION_STATUS_COLS = [
    "season", "round", "session_type", "session_time_ms", "status",
]
RESULTS_COLS = [
    "season", "round", "session_type", "driver_number", "driver_code", "full_name",
    "team", "team_color", "position", "classified_position", "grid_position",
    "q1_ms", "q2_ms", "q3_ms", "race_time_ms", "status", "points",
]


def _connect():
    return psycopg2.connect(
        host=settings.pg_host, port=settings.pg_port,
        user=settings.pg_user, password=settings.pg_password,
        dbname=settings.pg_database,
    )


class _Db:
    """Conexão psycopg2 que reconecta se cair.

    Um replay lento mantém a conexão aberta por muito tempo; uma queda
    momentânea de rede não deve derrubar o job inteiro.
    """

    def __init__(self, autocommit: bool):
        self._autocommit = autocommit
        self._open()

    def _open(self) -> None:
        self.conn = _connect()
        self.conn.autocommit = self._autocommit
        self.cur = self.conn.cursor()

    def execute_values(self, sql: str, rows: list[tuple], *, max_attempts: int = 4) -> None:
        if not rows:
            return
        for attempt in range(1, max_attempts + 1):
            try:
                execute_values(self.cur, sql, rows)
                return
            except psycopg2.OperationalError as exc:
                if attempt == max_attempts:
                    raise
                log.warning("db_reconnect", attempt=attempt, err=str(exc))
                with contextlib.suppress(Exception):
                    self.conn.close()
                time.sleep(min(2 ** attempt, 15))
                self._open()

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.cur.close()
        self.conn.close()


def _tel_row(row, meta) -> tuple:
    st = row.get("SessionTime")
    return (
        meta["season"], meta["round"], meta["session_type"],
        str(row.get("DriverNumber", "")), str(row.get("Driver", "")), str(row.get("Team", "")),
        _i(row.get("LapNumber")),
        int(st.total_seconds() * 1000) if pd.notna(st) else 0,
        _f(row.get("Speed")), _f(row.get("Throttle")), _b(row.get("Brake")),
        _i(row.get("nGear")), _f(row.get("RPM")), _i(row.get("DRS")),
        _f(row.get("X")), _f(row.get("Y")), _f(row.get("Z")),
    )


def _lap_row(row, meta) -> tuple:
    return (
        meta["season"], meta["round"], meta["session_type"],
        str(row.get("DriverNumber", "")), str(row.get("Driver", "")), str(row.get("Team", "")),
        _i(row.get("LapNumber")), _ms(row.get("LapTime")),
        _ms(row.get("Sector1Time")), _ms(row.get("Sector2Time")), _ms(row.get("Sector3Time")),
        _s(row.get("Compound")), _i(row.get("TyreLife")), _b(row.get("IsPersonalBest")),
        _ms(row.get("PitInTime")), _ms(row.get("PitOutTime")), _s(row.get("TrackStatus")),
    )


def _weather_row(row, meta) -> tuple:
    st = row.get("Time")
    return (
        meta["season"], meta["round"], meta["session_type"],
        _ms(st) if hasattr(st, "total_seconds") else None,
        _f(row.get("AirTemp")), _f(row.get("Humidity")), _f(row.get("Pressure")),
        _b(row.get("Rainfall")), _f(row.get("TrackTemp")),
        _i(row.get("WindDirection")), _f(row.get("WindSpeed")),
    )


def _rcm_row(row, meta) -> tuple:
    return (
        meta["season"], meta["round"], meta["session_type"],
        _dt(row.get("Time")), _s(row.get("Category")), _s(row.get("Message")),
        _s(row.get("Status")), _s(row.get("Flag")), _s(row.get("Scope")),
        _i(row.get("Sector")), _s(row.get("RacingNumber")), _i(row.get("Lap")),
    )


def _track_status_row(row, meta) -> tuple:
    st = row.get("Time")
    return (
        meta["season"], meta["round"], meta["session_type"],
        _ms(st) if hasattr(st, "total_seconds") else None,
        _s(row.get("Status")), _s(row.get("Message")),
    )


def _session_status_row(row, meta) -> tuple:
    st = row.get("Time")
    return (
        meta["season"], meta["round"], meta["session_type"],
        _ms(st) if hasattr(st, "total_seconds") else None,
        _s(row.get("Status")),
    )


def _results_row(row, meta) -> tuple:
    return (
        meta["season"], meta["round"], meta["session_type"],
        str(row.get("DriverNumber", "")), str(row.get("Abbreviation", "")), _s(row.get("FullName")),
        _s(row.get("TeamName")), _s(row.get("TeamColor")),
        _f(row.get("Position")), _s(row.get("ClassifiedPosition")), _f(row.get("GridPosition")),
        _ms(row.get("Q1")), _ms(row.get("Q2")), _ms(row.get("Q3")),
        _ms(row.get("Time")), _s(row.get("Status")), _f(row.get("Points")),
    )


def load(gp: str, session_type: str, driver: str | None, limit: int | None,
         mode: str, speed: float) -> None:
    session = fastf1.get_session(settings.f1_season, gp, session_type)
    session.load(telemetry=True, laps=True, weather=True, messages=True)
    meta = {"season": settings.f1_season, "round": int(session.event["RoundNumber"]),
            "session_type": session_type}

    laps = session.laps
    if driver:
        laps = laps.pick_drivers(driver)

    tel_rows: list[tuple] = []
    for _, lap in laps.iterlaps():
        try:
            tel = lap.get_car_data(interpolate_edges=True)
            tel["Driver"], tel["Team"] = lap["Driver"], lap["Team"]
            tel["DriverNumber"], tel["LapNumber"] = lap["DriverNumber"], lap["LapNumber"]
            for _, r in tel.iterrows():
                tel_rows.append(_tel_row(r, meta))
        except Exception as exc:
            log.warning("tel_skip", err=str(exc))
    tel_rows.sort(key=lambda x: x[7])           # ordena por session_time_ms
    if limit:
        tel_rows = tel_rows[:limit]

    # Tempo de conclusão da volta: usado para intercalar as voltas na mesma
    # linha do tempo da telemetria. Volta sem Time vai para o início (seq 0).
    lap_rows_timed: list[tuple[int, tuple]] = []
    laps_missing_time = 0
    for _, r in laps.iterrows():
        t = _ms(r.get("Time"))
        if t is None:
            laps_missing_time += 1
            t = 0
        lap_rows_timed.append((t, _lap_row(r, meta)))
    if laps_missing_time:
        log.warning("laps_missing_time", count=laps_missing_time)

    def _safe_rows(builder, df_attr: str) -> list[tuple]:
        try:
            df = getattr(session, df_attr)
            if df is None or df.empty:
                return []
            return [builder(r, meta) for _, r in df.iterrows()]
        except Exception as exc:
            log.warning("session_dataset_skip", dataset=df_attr, err=str(exc))
            return []

    weather_rows = _safe_rows(_weather_row, "weather_data")
    rcm_rows = _safe_rows(_rcm_row, "race_control_messages")
    track_status_rows = _safe_rows(_track_status_row, "track_status")
    session_status_rows = _safe_rows(_session_status_row, "session_status")
    results_rows = _safe_rows(_results_row, "results")

    db = _Db(autocommit=(mode == "replay"))     # no replay, commit por linha

    def _insert_session_level_datasets() -> None:
        for table, cols, rows in [
            ("weather_events", WEATHER_COLS, weather_rows),
            ("race_control_messages", RCM_COLS, rcm_rows),
            ("track_status_events", TRACK_STATUS_COLS, track_status_rows),
            ("session_status_events", SESSION_STATUS_COLS, session_status_rows),
            ("session_results", RESULTS_COLS, results_rows),
        ]:
            db.execute_values(f"INSERT INTO {table} ({','.join(cols)}) VALUES %s", rows)

    if mode == "batch":
        db.execute_values(f"INSERT INTO telemetry_events ({','.join(TEL_COLS)}) VALUES %s", tel_rows)
        db.execute_values(f"INSERT INTO lap_events ({','.join(LAP_COLS)}) VALUES %s",
                           [r for _, r in lap_rows_timed])
        _insert_session_level_datasets()
        db.commit()
        log.info("batch_done", gp=gp, telemetry=len(tel_rows), laps=len(lap_rows_timed),
                  weather=len(weather_rows), race_control=len(rcm_rows),
                  track_status=len(track_status_rows), session_status=len(session_status_rows),
                  results=len(results_rows))
    else:  # replay: telemetria e voltas na mesma linha do tempo
        tel_sql = f"INSERT INTO telemetry_events ({','.join(TEL_COLS)}) VALUES %s"
        lap_sql = f"INSERT INTO lap_events ({','.join(LAP_COLS)}) VALUES %s"

        timeline: list[tuple[int, str, tuple]] = (
            [(r[7], "tel", r) for r in tel_rows]
            + [(t, "lap", r) for t, r in lap_rows_timed]
        )
        timeline.sort(key=lambda x: x[0])

        prev_ms = None
        n_tel = n_lap = 0
        for i, (cur_ms, kind, row) in enumerate(timeline):
            if prev_ms is not None:
                delay = max(0.0, (cur_ms - prev_ms) / 1000.0 / speed)
                time.sleep(min(delay, 2.0))     # teto de 2s entre eventos
            if kind == "tel":
                db.execute_values(tel_sql, [row])
                n_tel += 1
            else:
                db.execute_values(lap_sql, [row])
                n_lap += 1
            prev_ms = cur_ms
            if i % 50 == 0:
                log.info("replay_progress", emitted=i, total=len(timeline))
        _insert_session_level_datasets()
        log.info("replay_done", gp=gp, telemetry=n_tel, laps=n_lap,
                  weather=len(weather_rows), race_control=len(rcm_rows),
                  track_status=len(track_status_rows), session_status=len(session_status_rows),
                  results=len(results_rows))

    db.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--gp", default="Bahrain")
    p.add_argument("--session", default="R")
    p.add_argument("--season", type=int, default=settings.f1_season)
    p.add_argument("--driver", default=None)
    p.add_argument("--limit", type=int, default=None, help="limite de linhas de telemetria (testes rapidos)")
    p.add_argument("--mode", choices=["batch", "replay"], default="batch")
    p.add_argument("--speed", type=float, default=20.0, help="fator de compressao de tempo no replay")
    args = p.parse_args()

    settings.f1_season = args.season
    log.info("pg_load_start", gp=args.gp, session=args.session, driver=args.driver,
             mode=args.mode, limit=args.limit)
    load(args.gp, args.session, args.driver, args.limit, args.mode, args.speed)


if __name__ == "__main__":
    main()
