"""Dispara e acompanha ingestões como subprocessos."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from streamlit_app import db

LOG_DIR = Path("/tmp/ingestion_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Dict de módulo (não st.session_state, que é recriado a cada rerun do Streamlit)
_RUNNING: dict[int, subprocess.Popen] = {}


def start_job(*, season: int, gp: str, session_type: str, driver: str | None,
              mode: str, speed: float, telemetry_limit: int | None) -> int:
    cmd = [
        sys.executable, "-m", "ingestion.producers.fastf1_to_postgres",
        "--gp", gp,
        "--session", session_type,
        "--season", str(season),
        "--mode", mode,
    ]
    if driver:
        cmd += ["--driver", driver]
    if mode == "replay":
        cmd += ["--speed", str(speed)]
    if telemetry_limit:
        cmd += ["--limit", str(telemetry_limit)]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    log_path = LOG_DIR / f"job_pid{proc.pid}.log"

    job_id = db.insert_job(
        season=season, gp=gp, session_type=session_type, driver=driver,
        mode=mode, speed=speed if mode == "replay" else None,
        telemetry_limit=telemetry_limit, pid=proc.pid, log_path=str(log_path),
    )
    _RUNNING[job_id] = proc

    # Thread separada: se o buffer do pipe encher, o subprocesso trava
    import threading

    def _drain() -> None:
        assert proc.stdout is not None  # garantido pelo stdout=PIPE acima
        with log_path.open("w") as f:
            for line in proc.stdout:
                f.write(line)
                f.flush()

    threading.Thread(target=_drain, daemon=True).start()

    return job_id


def stop_job(job_id: int) -> bool:
    """Encerra o subprocesso e marca o job como cancelado.

    Retorna False se o handle foi perdido (container reiniciado): o job pode
    seguir rodando órfão, e a UI precisa dizer isso em vez de fingir sucesso.
    """
    proc = _RUNNING.get(job_id)
    if proc is None:
        return False

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)

    db.mark_job_cancelled(job_id)
    del _RUNNING[job_id]
    return True


def poll_running_jobs(running_rows: list[tuple[int, str]]) -> None:
    """Reconcilia os jobs marcados como 'running' no Postgres com os
    subprocessos vivos. Handle perdido vira status 'unknown'."""
    for job_id, log_path in running_rows:
        proc = _RUNNING.get(job_id)
        if proc is None:
            db.mark_job_unknown(job_id)
            continue

        exit_code = proc.poll()
        if exit_code is None:
            continue  # ainda rodando

        success = exit_code == 0
        error_message = None if success else tail_log(log_path, lines=15)
        db.mark_job_finished(job_id, success=success, error_message=error_message)
        del _RUNNING[job_id]


def tail_log(log_path: str, *, lines: int = 40) -> str:
    p = Path(log_path)
    if not p.exists():
        return "(log ainda não disponível)"
    content = p.read_text(errors="replace").splitlines()
    return "\n".join(content[-lines:]) if content else "(sem saída ainda)"
