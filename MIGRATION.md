# Migração para produção — AWS S3 + Databricks Free Edition

Guia da migração do lakehouse local (Docker Compose) para a nuvem.
**Caminho escolhido:** manter o CDC (Debezium/Kafka) repontado para o **S3 real**;
**Databricks Free Edition** com **Unity Catalog** (substitui Hive Metastore + Trino);
**Bronze always-on como Databricks Job nativo** (Auto Loader, File arrival trigger);
**Silver/Gold orquestrados pelo Airflow local**, rodando `dbt-databricks` contra o
SQL Warehouse — o Databricks Free Edition serverless não suporta o `dbt_task`
nativo (ver nota abaixo), então o Airflow assume esse papel específico.

> **Por que Airflow para Silver/Gold e não um Job `dbt_task` nativo?** Foi
> tentado primeiro. O SQL Warehouse serverless do Free Edition não suporta o
> canal "Client-1" REPL que o `dbt_task` exige para subir — erro confirmado via
> API (`INVALID_PARAMETER_VALUE` no cluster launch), uma limitação real da
> plataforma, não um erro de config. Rodar `dbt-databricks` de *fora* do
> Databricks (no Airflow, via `BashOperator`) e falar com o SQL Warehouse pela
> rede contorna essa limitação — sem Docker-out-of-Docker, dbt-databricks
> instalado direto na imagem do Airflow. A DAG `f1_lakehouse_pipeline` tem uma
> task por modelo (`dbt build --select <model>`), não um passo monolítico.

---

## Mapa local → cloud

| Local (Docker Compose) | Cloud (AWS + Databricks) | Muda? |
|---|---|---|
| Postgres + Debezium + Kafka + S3 Sink | idem (local ou EC2) — só o Sink repontado | config |
| MinIO (`s3a://`) | **S3 real** (`s3://<bucket>/`) | só endpoint |
| Spark autoloader (batch, 1x + Airflow hourly) | **Auto Loader** (`cloudFiles`), Job **File arrival**, always-on | vira event-driven |
| Hive Metastore + Postgres | **Unity Catalog** (gerenciado) | some (zero setup) |
| Trino (serving) | **Databricks SQL** (gerenciado) | some (zero setup) |
| dbt session-mode | **dbt-databricks** | só `profiles` |
| — | **Airflow (local)** orquestra Silver/Gold: 1 task por modelo dbt (`dbt build --select`), rodando `dbt-databricks` contra o SQL Warehouse | mantido — ver nota acima sobre a limitação do `dbt_task` nativo |
| Tabelas por path Delta | `f1.bronze/silver/gold.*` (UC) | nomes |

> **O CDC local nunca precisou de orquestrador:** os Kafka Connect connectors
> (Debezium source + S3 Sink) rodam continuamente sozinhos, uma vez registrados —
> não há nenhum passo agendado ali. O Airflow entra especificamente para
> orquestrar **Silver/Gold**, e só porque o `dbt_task` nativo do Databricks não
> funciona neste workspace serverless (ver nota no topo do documento). Se o
> Free Edition suportasse `dbt_task`, o Airflow não seria necessário — Bronze e
> Silver/Gold rodariam como dois Jobs Databricks desacoplados, cada um com seu
> próprio trigger nativo.

---

## Variáveis

| Variável | Valor | Onde é usado |
|---|---|---|
| Bucket S3 | **`f1-lakehouse-galgo`** | s3-sink-connector.aws.json, notebooks Databricks, SQL setup |
| Região AWS | `us-east-2` | bucket + s3-sink |
| `DATABRICKS_HOST` | `dbc-xxxx.cloud.databricks.com` | profiles.databricks.yml — preencher ainda |
| `DATABRICKS_HTTP_PATH` | `/sql/1.0/warehouses/<id>` | profiles.databricks.yml — preencher ainda |
| `DATABRICKS_TOKEN` | PAT (Settings → Developer) | profiles.databricks.yml — preencher ainda |

> O bucket já está fixado (`f1-lakehouse-galgo`) em todos os artefatos. Faltam só as 3 variáveis do Databricks acima, que dependem do workspace/SQL Warehouse criados na Fase 3.

---

## Passo a passo

### Fase 1 — AWS  `[VOCÊ]`
1. **S3** (us-east-2) → criar bucket `f1-lakehouse-galgo`, Block Public Access ligado.
2. **IAM user** para o S3 Sink escrever no bucket → gerar Access Key + Secret
   (política mínima: `s3:PutObject`, `s3:ListBucket` no bucket).

### Fase 2 — Repontar o CDC para o S3 real  `[artefato pronto]`
3. Colocar as chaves do IAM user no `.env` do projeto:
   ```
   AWS_ACCESS_KEY_ID=AKIA...
   AWS_SECRET_ACCESS_KEY=...
   ```
   (o container `debezium-connect` já expõe essas envs → o Sink as usa)
4. Registrar o connector cloud (mantém o local intacto):
   ```bash
   curl -X POST -H "Content-Type: application/json" \
     --data @infra/debezium/s3-sink-connector.aws.json \
     http://localhost:8083/connectors
   ```
5. Rodar uma corrida (ex.: Mônaco 2025) → os `.avro` aparecem em `s3://f1-lakehouse-galgo/cdc/...`

### Fase 3 — Databricks  `[VOCÊ cria conta + artefatos prontos]`
6. **External Location** (UI): Catalog → External Data → External Locations →
   **Set up Automatically** → apontar para `s3://f1-lakehouse-galgo/` → rodar o CloudFormation
   que o Databricks gera (cria a IAM role na sua AWS).
7. Rodar `databricks/01_unity_catalog_setup.sql` → cria catálogo `f1` + schemas.
   O `LIST 's3://f1-lakehouse-galgo/cdc/'` deve listar os `.avro`.
8. **Bronze — Auto Loader always-on via File Arrival trigger** (não um Continuous
   loop — mais barato: só roda quando chega arquivo novo, zero compute entre chegadas):
   - Colar `databricks/02_bronze_autoloader.py` num notebook.
   - Jobs & Pipelines → **New Job** → nome `f1_bronze_streaming` → task = esse notebook.
   - **Schedules & Triggers** → Trigger type: **File arrival** →
     Storage location: `s3://f1-lakehouse-galgo/cdc/` → Trigger Status: **Active**.
   - Pronto — o Databricks monitora o S3 nativamente e dispara o Job sozinho a cada
     chegada de `.avro`. Cada execução processa o pendente (`trigger(availableNow=True)`
     no notebook) e encerra. Cria `f1.bronze.telemetry` / `f1.bronze.laps`.
9. **dbt Silver/Gold via Airflow local** (não é um Job Databricks — roda de fora):
   - `dbt-databricks` já está instalado na imagem do Airflow ([infra/docker/airflow/Dockerfile](infra/docker/airflow/Dockerfile))
   - usar `dbt/f1_lakehouse/profiles.yml` (target `dev`, `type: databricks`, credenciais via `env_var()`)
   - **1 ajuste no `dbt_project.yml`:** sem `+location_root:` em silver/gold
     (no Databricks as tabelas são gerenciadas pelo UC, sem path S3 explícito);
     `incremental_strategy: merge` (não `insert_overwrite` — o SQL Warehouse
     serverless não expõe `spark.sql.sources.partitionOverwriteMode`)
   - a DAG `f1_lakehouse_pipeline` ([airflow/dags/f1_pipeline.py](airflow/dags/f1_pipeline.py))
     roda `dbt build --select <model>` por modelo → cria `f1.silver.*` e `f1.gold.*`
10. **Serving:** SQL Editor do Databricks (substitui o Trino):
    ```sql
    SELECT * FROM f1.gold.fact_driver_season_summary ORDER BY fastest_lap_s LIMIT 10;
    ```
11. **Orquestração — dois mecanismos independentes:**
    - `f1_bronze_streaming` (Databricks Job) — Trigger: **File arrival** (passo 8),
      dispara sozinho a cada chegada de arquivo no S3. Sempre always-on, nunca
      precisa de agendamento manual.
    - `f1_lakehouse_pipeline` (**Airflow local**, `docker compose up airflow-scheduler
      airflow-webserver`) — schedule `@hourly`, uma task por modelo dbt, lê o que
      a Bronze já tiver acumulado no momento. Ver nota no topo deste documento
      sobre por que Silver/Gold não usa um Job `dbt_task` nativo.

---

## Custos (Free Edition + AWS free tier)

- **S3:** landing de poucos GB → dentro/perto do free tier (5 GB); depois ~US$0,02/GB.
- **Databricks Free Edition:** compute serverless gratuito com cota de uso.
- **CDC local:** continua na sua máquina (grátis). Se mover para EC2/MSK, aí há custo.

> Não é "produção enterprise" (sem SLA/HA), mas roda o pipeline inteiro na nuvem
> gerenciada como demonstração de portfólio — que é o objetivo.

---

## Artefatos desta migração

| Arquivo | Papel |
|---|---|
| `infra/debezium/s3-sink-connector.aws.json` | S3 Sink repontado para o S3 real |
| `databricks/01_unity_catalog_setup.sql` | Cria catálogo `f1` + schemas |
| `databricks/02_bronze_autoloader.py` | Bronze via Auto Loader (cloudFiles), **always-on** — deploy como Job com trigger **File arrival** |
| `dbt/f1_lakehouse/profiles.yml` | Perfil dbt-databricks (`target: dev`, credenciais via `env_var()`) |
| `airflow/dags/f1_pipeline.py` | DAG `f1_lakehouse_pipeline` — uma task por modelo dbt contra o SQL Warehouse |
| `infra/docker/airflow/Dockerfile` | Imagem Airflow com `dbt-databricks` instalado (sem DooD, sem socket Docker) |
