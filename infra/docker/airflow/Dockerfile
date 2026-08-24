FROM apache/airflow:2.9.2-python3.11

# dbt-databricks: the DAG runs `dbt run` / `dbt test` directly via BashOperator,
# connecting OUT to the Databricks SQL Warehouse (profiles.yml uses env_var(),
# reading DATABRICKS_HOST/TOKEN/HTTP_PATH/CATALOG from this container's .env).
#
# Why not the native Databricks `dbt_task` (Jobs API) instead: this project's
# Databricks Free Edition workspace doesn't support the serverless "Client-1"
# REPL channel that dbt_task requires to launch — a real platform limitation,
# not a config bug (confirmed via API: INVALID_PARAMETER_VALUE at cluster
# launch). Running dbt from OUTSIDE Databricks (here, in Airflow) and just
# talking to the SQL Warehouse over the network sidesteps that limitation
# entirely — already validated working end-to-end (dbt run + dbt test PASS).
USER airflow
RUN pip install --no-cache-dir "dbt-databricks==1.8.4"
