# F1 Lakehouse — CDC → Kafka → Databricks → dbt

Pipeline de dados que pega telemetria real de Fórmula 1 (API [FastF1](https://github.com/theOehrly/Fast-F1)), captura cada mudança via **CDC** e leva até um lakehouse na nuvem em camadas **Bronze/Silver/Gold**.

**Caminho:** PostgreSQL → Debezium (CDC) → Kafka (Avro + Schema Registry) → S3 Sink → AWS S3 → Databricks Auto Loader → Bronze (Delta + Unity Catalog) → dbt → Silver/Gold, orquestrado por Airflow. Um painel Streamlit dispara as ingestões e é auditado pelo mesmo CDC.

> 📖 **[Leia o artigo completo aqui](LINK_DO_ARTIGO)** — a motivação, as decisões de arquitetura e os problemas reais encontrados no caminho. Este README é só a referência técnica rápida.

---

## Escopo do projeto

O foco aqui foi a **ingestão**: montar um pipeline CDC real, de ponta a ponta, com os problemas de verdade que aparecem nesse caminho (ordenação por LSN, propagação de deletes, schema evolution, Free Edition serverless). Os modelos dbt (Silver/Gold) e a DAG do Airflow resolvem o problema e têm testes, mas não foram o objetivo — não há refino de performance, particionamento fino ou modelagem dimensional além do necessário para provar o pipeline ponta a ponta. Se fosse produção, essa é a próxima frente.

---

## Arquitetura

```
┌─────────────────────────── LOCAL (Docker Compose) ───────────────────────────┐
│  FastF1 API → Streamlit/CLI → PostgreSQL (OLTP) → Debezium (WAL/CDC)         │
│                                                        │ Avro                 │
│                                    Schema Registry ◀── Kafka ──▶ S3 Sink      │
└───────────────────────────────────────────────────────┼─────────────────────┘
                                                          │ .avro
                                                          ▼
┌──────────────────────────── CLOUD (AWS + Databricks) ────────────────────────┐
│  s3://.../cdc/ → Auto Loader (File arrival) → Bronze (Delta, Unity Catalog)   │
│                                                       │                       │
│  Airflow (local) → dbt build --select <model> → Silver/Gold (SQL Warehouse)  │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Por que híbrido?** O CDC (Debezium/Kafka/S3 Sink) roda local, de graça, na máquina — só o Sink escreve no S3 real. O lakehouse (Bronze/Silver/Gold, catálogo, serving) roda no Databricks com Unity Catalog gerenciado. Passo a passo da migração local → cloud em [MIGRATION.md](MIGRATION.md).

---

## Stack

| Camada | Tecnologia | Papel |
|---|---|---|
| Fonte OLTP | PostgreSQL 16 | `wal_level=logical` expõe o WAL ao Debezium |
| Painel | Streamlit | Dispara ingestões, mostra histórico e cobertura |
| Ingestão | Python + FastF1 | Busca dados reais de corrida, insere no Postgres |
| CDC | Debezium (Postgres Source Connector) | Lê o WAL via `pgoutput`, emite cada mudança como evento |
| Schema | Confluent Schema Registry | Contrato Avro entre produtor e consumidor |
| Message bus | Apache Kafka + Zookeeper | Tópicos `f1cdc.public.*`, retenção de 7 dias |
| Landing | Confluent S3 Sink Connector | Grava `.avro` no S3, particionado por hora |
| Bronze | Databricks Auto Loader | `cloudFiles`, trigger por chegada de arquivo, always-on sem cluster 24/7 |
| Formato | Delta Lake | ACID, time-travel, `MERGE`, schema evolution |
| Catálogo | Unity Catalog | `f1.bronze` / `f1.silver` / `f1.gold` |
| Transformação | dbt (`dbt-databricks`) | Silver/Gold contra o SQL Warehouse |
| Orquestração | Apache Airflow | Uma task por modelo dbt (`dbt build` = run + test) |

Detalhes de cada camada (por que Avro, como funciona a propagação de deletes na Silver, o padrão SCD2 "islands" nas dimensões, etc.) estão comentados nos próprios arquivos — ver `dbt/f1_lakehouse/models/` e `databricks/`.

---

## Rodando local

```bash
docker compose build
docker compose up -d

# Kafka tem auto-create de topico desligado — precisa criar antes de registrar
docker exec kafka bash -c "$(cat infra/scripts/create_topics.sh)"
bash infra/scripts/register_connectors.sh

# Carregar uma corrida (ou usar o painel em :8501)
docker compose run --rm ingestion-worker \
  python -m ingestion.producers.fastf1_to_postgres --gp Bahrain --session R --driver VER
```

Com `make` instalado, os mesmos passos viram `make build / up / topics / register`. Ver `make help` para o resto (status, replay, logs).

| Serviço | URL | Credenciais |
|---|---|---|
| Streamlit | http://localhost:8501 | — |
| Airflow | http://localhost:8085 | admin / admin |
| Kafka UI | http://localhost:8082 | — |
| Debezium REST | http://localhost:8083/connectors | — |
| Postgres OLTP | localhost:5432 | f1user / f1pass / f1oltp |

Copie `.env.example` para `.env` (credenciais AWS + Databricks) antes de subir.

---

## Estrutura

```
airflow/dags/          DAG f1_lakehouse_pipeline (dbt build por-modelo → Databricks)
databricks/            Setup Unity Catalog + Bronze Auto Loader
dbt/f1_lakehouse/      Projeto dbt: silver (7 models), gold (7 models), tests
ingestion/             Producer FastF1 → Postgres
streamlit_app/         Painel de controle de ingestão
infra/                 Docker, configs dos connectors, schema OLTP, scripts
docker-compose.yml     Stack local de CDC
MIGRATION.md           Guia local → AWS S3 + Databricks
```
