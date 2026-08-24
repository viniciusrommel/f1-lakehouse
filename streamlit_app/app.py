"""Painel de controle: dispara ingestões FastF1 -> Postgres com filtros,
mostra o histórico de execuções e a cobertura das tabelas do CDC.

As próprias execuções são capturadas pelo CDC (tabela ingestion_jobs).
"""
from __future__ import annotations

import sys
from pathlib import Path

import fastf1
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from streamlit_app import db, jobs  # noqa: E402

fastf1.Cache.enable_cache("/tmp/fastf1_cache")

st.set_page_config(page_title="F1 Lakehouse — Painel de Controle", layout="wide")

SESSION_LABELS = {
    "Treino Livre 1":       "FP1",
    "Treino Livre 2":       "FP2",
    "Treino Livre 3":       "FP3",
    "Classificação":        "Q",
    "Sprint":                "S",
    "Classificação Sprint": "SQ",
    "Corrida":               "R",
}

db.bootstrap()


@st.cache_data(ttl=3600, show_spinner="Carregando calendário da temporada...")
def load_schedule(season: int) -> list[str]:
    try:
        sched = fastf1.get_event_schedule(season, include_testing=False)
        return sched["EventName"].tolist()
    except Exception:
        return []


st.title("F1 Lakehouse — Painel de Controle")
st.caption(
    "Insere corridas reais (FastF1) no Postgres. Debezium captura via CDC → "
    "Kafka → S3 → Databricks Auto Loader → Bronze. Toda execução daqui também "
    "é rastreada como CDC (tabela `ingestion_jobs`)."
)

tab_new, tab_runs, tab_coverage = st.tabs(["Nova Ingestão", "Execuções", "Cobertura de Dados"])

with tab_new:
    col1, col2 = st.columns(2)

    with col1:
        season = st.selectbox("Temporada", options=list(range(2025, 2017, -1)), index=0)
        events = load_schedule(season)
        if events:
            gp = st.selectbox("Grande Prêmio", options=events)
        else:
            gp = st.text_input("Grande Prêmio (nome não encontrado no calendário — digite manualmente)", value="Bahrain")

        session_label = st.selectbox("Sessão", options=list(SESSION_LABELS.keys()), index=6)
        session_type = SESSION_LABELS[session_label]

    with col2:
        todos_pilotos = st.checkbox("Todos os pilotos", value=True)
        driver = None
        if not todos_pilotos:
            driver = st.text_input(
                "Código do piloto (3 letras)", value="",
                help="Ex.: VER, HAM, BOR, LEC. Carregue com 'Todos' uma vez para ver os códigos na aba Cobertura.",
            ).strip().upper() or None

        mode = st.radio("Modo", options=["batch", "replay"], horizontal=True,
                         help="batch = insere tudo de uma vez. replay = ritmo real da corrida, simula streaming ao vivo.")
        speed = 20.0
        if mode == "replay":
            speed = st.slider("Velocidade do replay (x mais rápido que o real)", 1.0, 100.0, 20.0)

        telemetry_limit = st.number_input(
            "Limite de linhas de telemetria (0 = sem limite)", min_value=0, value=0, step=100,
            help="Útil para testes rápidos sem esperar a sessão inteira carregar.",
        )

    st.divider()
    if st.button("Iniciar Ingestão", type="primary", use_container_width=True):
        if not gp:
            st.error("Informe o Grande Prêmio.")
        else:
            job_id = jobs.start_job(
                season=season, gp=gp, session_type=session_type, driver=driver,
                mode=mode, speed=speed, telemetry_limit=telemetry_limit or None,
            )
            st.success(f"Job #{job_id} iniciado — {gp} {season}, {session_label}, "
                       f"piloto: {driver or 'todos'}. Acompanhe na aba **Execuções**.")

with tab_runs:
    if st.button("Atualizar"):
        st.cache_data.clear()

    jobs_df = db.fetch_jobs()
    running_rows = [
        (row.id, row.log_path)
        for row in jobs_df.itertuples()
        if row.status == "running"
    ]
    if running_rows:
        jobs.poll_running_jobs(running_rows)
        jobs_df = db.fetch_jobs()

    if jobs_df.empty:
        st.info("Nenhuma ingestão executada ainda.")
    else:
        st.dataframe(
            jobs_df[["id", "status", "season", "gp", "session_type", "driver",
                     "mode", "duration_s", "created_at", "finished_at"]],
            use_container_width=True, hide_index=True,
        )

        st.subheader("Ver log / gerenciar uma execução")
        job_id_to_view = st.number_input("Job ID", min_value=1, step=1)
        row = jobs_df[jobs_df["id"] == job_id_to_view]

        col_log, col_cancel = st.columns([3, 1])
        with col_log:
            if st.button("Ver log"):
                if row.empty:
                    st.warning("Job não encontrado.")
                else:
                    log_path = row.iloc[0]["log_path"]
                    st.code(jobs.tail_log(log_path, lines=60), language="text")
                    error_msg = row.iloc[0]["error_message"]
                    if isinstance(error_msg, str) and error_msg:
                        st.error(f"Erro registrado:\n\n```\n{error_msg}\n```")

        with col_cancel:
            is_running = not row.empty and row.iloc[0]["status"] == "running"
            if st.button("Cancelar", disabled=not is_running, use_container_width=True):
                if jobs.stop_job(int(job_id_to_view)):
                    st.success(f"Job #{job_id_to_view} cancelado.")
                    st.rerun()
                else:
                    st.warning(
                        "Não foi possível cancelar — o handle do processo foi perdido "
                        "(container reiniciado?). O job pode continuar rodando como órfão."
                    )

with tab_coverage:
    st.caption("O que já está carregado no Postgres (fonte do CDC) — 7 tabelas capturadas, cada uma flui até a Bronze.")

    st.subheader("Linhas por tabela")
    counts_df = db.fetch_table_counts()
    cols = st.columns(len(counts_df))
    for col, (_, row) in zip(cols, counts_df.iterrows(), strict=True):
        col.metric(row["tabela"], f"{row['linhas']:,}".replace(",", "."))

    st.divider()

    coverage_df = db.fetch_coverage()
    if coverage_df.empty:
        st.info("Nenhum dado de telemetria/laps carregado ainda.")
    else:
        st.subheader("Cobertura por temporada/corrida/sessão")
        st.dataframe(coverage_df, use_container_width=True, hide_index=True)

        st.subheader("Detalhe de uma sessão")
        c1, c2, c3 = st.columns(3)
        with c1:
            # int(x): .unique() devolve numpy.int64, que o psycopg2 não aceita
            seasons = sorted({int(x) for x in coverage_df["season"].unique()}, reverse=True)
            season_sel = st.selectbox("Temporada", seasons)
        with c2:
            rounds_for_season = sorted({int(x) for x in coverage_df[coverage_df["season"] == season_sel]["round"].unique()})
            round_sel = st.selectbox("Round", rounds_for_season)
        with c3:
            sessions_for_round = sorted(
                coverage_df[(coverage_df["season"] == season_sel) & (coverage_df["round"] == round_sel)]["session_type"].unique()
            )
            session_sel = st.selectbox("Sessão", sessions_for_round)

        sub_drivers, sub_results = st.tabs(["Pilotos (telemetria)", "Classificação (results)"])
        with sub_drivers:
            drivers_df = db.fetch_drivers_for(season_sel, round_sel, session_sel)
            st.dataframe(drivers_df, use_container_width=True, hide_index=True)
        with sub_results:
            results_df = db.fetch_results_for(season_sel, round_sel, session_sel)
            if results_df.empty:
                st.info("Sem resultados carregados para esta sessão.")
            else:
                st.dataframe(results_df, use_container_width=True, hide_index=True)
