#!/bin/bash
# Cria os topicos Kafka necessarios. Precisa rodar antes do primeiro
# register_connectors.sh, pois KAFKA_AUTO_CREATE_TOPICS_ENABLE=false.
#
# Uso:
#   docker exec kafka bash -c "$(cat infra/scripts/create_topics.sh)"

BOOTSTRAP="localhost:9092"

create_topic() {
  local topic=$1
  local partitions=$2
  kafka-topics --bootstrap-server $BOOTSTRAP \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms=604800000
  echo "  $topic ($partitions particoes)"
}

echo "=== F1 Platform — Provisionamento de Topicos Kafka ==="
echo ""

# Nomenclatura: <topic.prefix>.public.<tabela> (ver f1-postgres-connector.json)
create_topic "f1cdc.public.telemetry_events"      6
create_topic "f1cdc.public.lap_events"            3
create_topic "f1cdc.public.ingestion_jobs"        1
create_topic "f1cdc.public.weather_events"        1
create_topic "f1cdc.public.race_control_messages" 1
create_topic "f1cdc.public.track_status_events"   1
create_topic "f1cdc.public.session_status_events" 1
create_topic "f1cdc.public.session_results"       1

echo ""
echo "=== Topicos ==="
kafka-topics --bootstrap-server $BOOTSTRAP --list
