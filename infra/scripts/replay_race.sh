#!/usr/bin/env bash
# Faz o replay de uma corrida com dois pilotos em paralelo, cada um em seu
# proprio container, escrevendo no Postgres ao mesmo tempo.
#
# Uso:
#   ./infra/scripts/replay_race.sh [GP] [SESSAO] [VELOCIDADE] [PILOTO1] [PILOTO2]
#
# Exemplos:
#   ./infra/scripts/replay_race.sh "São Paulo" R 50 VER HAM
#   ./infra/scripts/replay_race.sh Bahrain R 100 NOR LEC

GP="${1:-São Paulo}"
SESSION="${2:-R}"
SPEED="${3:-50}"
D1="${4:-VER}"
D2="${5:-HAM}"

echo "F1 Race Replay — GP: $GP | Sessão: $SESSION | Velocidade: ${SPEED}x | Pilotos: $D1 vs $D2"

run_driver() {
  local driver=$1
  echo "Iniciando $driver..."
  docker compose run --rm ingestion-worker \
    python -m ingestion.producers.fastf1_to_postgres \
    --gp "$GP" --session "$SESSION" --driver "$driver" \
    --mode replay --speed "$SPEED" \
    2>&1 | sed "s/^/[$driver] /"
  echo "$driver concluido"
}

run_driver "$D1" &
PID1=$!

run_driver "$D2" &
PID2=$!

wait $PID1
STATUS1=$?
wait $PID2
STATUS2=$?

echo ""
if [ $STATUS1 -eq 0 ] && [ $STATUS2 -eq 0 ]; then
  echo "Replay concluido — dados de $D1 e $D2 fluindo por Kafka -> S3."
  echo "Bronze capta automaticamente; Airflow roda Silver/Gold de hora em hora."
else
  echo "Um ou mais pilotos falharam (codigos de saida: $D1=$STATUS1, $D2=$STATUS2)"
  exit 1
fi
