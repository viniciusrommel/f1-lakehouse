# Arquitetura hibrida: a ingestao CDC e a orquestracao Airflow rodam local
# (este compose); o lakehouse (Bronze, Silver, Gold) roda no Databricks.
#
# Primeira execucao:  make build -> make up -> make topics -> make register

.DEFAULT_GOAL := help
.PHONY: help build up down restart topics register status replay replay-brazil logs

help:
	@echo ""
	@echo "  F1 Lakehouse — comandos disponiveis"
	@echo "  ─────────────────────────────────────────────────────────────"
	@echo "  make build          Constroi as imagens Docker"
	@echo "  make up             Sobe o ambiente local"
	@echo "  make down           Para todos os containers"
	@echo "  make restart        down + up"
	@echo ""
	@echo "  make topics         Cria os topicos Kafka (auto-create esta desligado)"
	@echo "  make register       Registra os connectors Debezium CDC + S3 Sink"
	@echo "  make status         Mostra o status dos connectors e o lag do consumo"
	@echo ""
	@echo "  make replay GP='Bahrain' SESSION=R DRIVER=VER SPEED=50"
	@echo "  make replay-brazil  VER x HAM, Sao Paulo 2023, 50x"
	@echo ""
	@echo "  make logs SVC=debezium-connect"
	@echo ""
	@echo "  A Bronze roda como Job do Databricks (Auto Loader, trigger por"
	@echo "  chegada de arquivo). Silver e Gold rodam pela DAG do Airflow,"
	@echo "  uma task por modelo dbt. Ver README.md e MIGRATION.md."
	@echo ""
	@echo "  Interfaces (apos 'make up'):"
	@echo "    Streamlit      http://localhost:8501"
	@echo "    Airflow        http://localhost:8085  (admin / admin)"
	@echo "    Kafka UI       http://localhost:8082"
	@echo "    Schema Reg     http://localhost:8081/subjects"
	@echo "    Debezium API   http://localhost:8083/connectors"
	@echo ""

build:
	docker compose build

up:
	docker compose up -d
	@echo ""
	@echo "Aguardando 60s ate os servicos ficarem saudaveis..."
	@sleep 60
	@echo "Pronto. Rode 'make register' para ativar os connectors."

down:
	docker compose down

restart: down up

topics:
	docker exec kafka bash -c "$$(cat infra/scripts/create_topics.sh)"

register:
	@echo "==> Registrando os connectors do Kafka Connect..."
	bash infra/scripts/register_connectors.sh

status:
	@echo "=== Status dos connectors ==="
	curl -s http://localhost:8083/connectors?expand=status
	@echo ""
	@echo "=== Lag do consumo do S3 Sink ==="
	docker exec kafka bash -c \
		"kafka-consumer-groups --bootstrap-server localhost:9092 \
		 --describe --group connect-f1-s3-sink-aws 2>&1" \
		| awk 'NR==1 || /f1cdc/'

GP      ?= Bahrain
SESSION ?= R
DRIVER  ?= VER
SPEED   ?= 50

replay:
	docker compose run --rm ingestion-worker \
		python -m ingestion.producers.fastf1_to_postgres \
		--gp "$(GP)" --session $(SESSION) --driver $(DRIVER) \
		--mode replay --speed $(SPEED)

replay-brazil:
	bash infra/scripts/replay_race.sh "São Paulo" R 50 VER HAM

SVC ?= debezium-connect
logs:
	docker logs -f $(SVC)
