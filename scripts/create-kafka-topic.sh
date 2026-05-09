#!/usr/bin/env bash
set -euo pipefail
TOPIC="${KAFKA_TOPIC:-crypto-trades}"
PARTS="${KAFKA_PARTITIONS:-3}"
COMPOSE_FILE="$(cd "$(dirname "$0")/.." && pwd)/docker-compose.yml"
docker compose -f "$COMPOSE_FILE" exec -T \
  -e TOPIC="$TOPIC" -e PARTS="$PARTS" kafka bash -lc \
  '/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --partitions "$PARTS" \
  --replication-factor 1'
