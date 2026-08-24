"""Um task por model dbt (`dbt build --select <model>`), rodado via Bash
e não como `dbt_task` nativo: o Free Edition serverless não suporta o
canal REPL que o dbt_task requer

Bronze roda em um Job separado, sempre ativo (Auto Loader; ver
databricks/02_bronze_autoloader.py) — não faz parte desta DAG.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

_DBT_PROJECT = "/opt/airflow/dbt/f1_lakehouse"
_DBT_OPTS = f"--project-dir {_DBT_PROJECT} --profiles-dir {_DBT_PROJECT}"

_SILVER_MODELS = [
    "telemetry",
    "laps",
    "weather",
    "race_control",
    "track_status",
    "session_status",
    "results",
]

_GOLD_DEPS = {
    "fact_driver_season_summary": ["laps"],
    "fact_race_pace_comparison": ["laps"],
    "sector_dominance": ["laps"],
    "race_weekend_summary": ["results", "laps", "weather", "race_control"],
    "dim_driver_scd2": ["results"],
    "dim_team_scd2": ["results"],
    "mart_driver_race_result": ["results", "laps"],
}

_GOLD_INTERNAL_DEPS = {
    "mart_driver_race_result": [
        "dim_driver_scd2", "dim_team_scd2",
        "fact_driver_season_summary", "fact_race_pace_comparison",
    ],
}

default_args = {
    "owner": "f1-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def _dbt_build_task(model: str) -> BashOperator:
    return BashOperator(
        task_id=model,
        bash_command=f"dbt build --select {model} {_DBT_OPTS}",
        doc_md=f"`dbt build --select {model}`",
    )


with DAG(
    dag_id="f1_lakehouse_pipeline",
    default_args=default_args,
    description="Silver e Gold via dbt build (run+test) no Databricks SQL Warehouse",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=4,
    tags=["f1", "lakehouse", "dbt", "databricks"],
) as dag:

    with TaskGroup(group_id="silver") as silver:
        silver_tasks = {m: _dbt_build_task(m) for m in _SILVER_MODELS}

    with TaskGroup(group_id="gold") as gold:
        gold_tasks = {m: _dbt_build_task(m) for m in _GOLD_DEPS}

    for model, deps in _GOLD_DEPS.items():
        for dep in deps:
            silver_tasks[dep] >> gold_tasks[model]

    for model, deps in _GOLD_INTERNAL_DEPS.items():
        for dep in deps:
            gold_tasks[dep] >> gold_tasks[model]
