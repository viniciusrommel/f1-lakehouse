#!/bin/bash
# Registra os connectors do Kafka Connect (Debezium CDC + S3 Sink).
# Rode a partir da raiz do projeto, com o debezium-connect saudavel:
#   bash infra/scripts/register_connectors.sh

CONNECT_URL="http://localhost:8083"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")/debezium"

register() {
  local name=$1
  local file=$2

  echo "==> Registrando $name ..."
  # DELETE seguido de POST: idempotente e evita depender de jq para
  # remover o wrapper {"name":...,"config":{...}} exigido pelo PUT /config.
  curl -s -X DELETE "$CONNECT_URL/connectors/$name" -o /dev/null
  curl -s -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$file"
  echo ""
}

echo "=== F1 Platform — Registro de Connectors do Kafka Connect ==="
echo ""

register "f1-postgres-cdc" "$INFRA_DIR/f1-postgres-connector.json"

# Precisa de AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY no .env.
register "f1-s3-sink-aws" "$INFRA_DIR/s3-sink-connector.aws.json"

echo ""
echo "=== Status dos Connectors ==="
curl -s "$CONNECT_URL/connectors?expand=status" | python3 -m json.tool 2>/dev/null || \
  curl -s "$CONNECT_URL/connectors"
