#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

TOPIC="${KAFKA_TOPIC:-crypto-trades}"
PARTS="${KAFKA_PARTITIONS:-3}"
TABLE="${HBASE_TABLE:-crypto_stream_metrics}"
CF="${HBASE_COLUMN_FAMILY:-m}"

MODE="${1:-all}"
case "$MODE" in
  setup|run|all|help|-h|--help) ;;
  *)
    echo "Unknown mode: $MODE" >&2
    echo "Usage: $(basename "$0") [setup|run|all]" >&2
    exit 1
    ;;
esac

usage() {
  cat <<EOF
Usage: $(basename "$0") [setup|run|all]

  setup   Docker (Kafka/HBase), Kafka topic, HBase table, Maven build, dashboard venv
  run     Producer + Spark + Streamlit (requires jars, SPARK_HOME, infra running)
  all     setup then run (default)

Environment: See README.md (KAFKA_*, HBASE_*, SPARK_HOME, METADATA_CSV, etc.)
EOF
}

wait_for_service() {
  local service="$1"
  local timeout="${2:-60}"
  local elapsed=0
  while true; do
    if docker compose -f "$ROOT/docker-compose.yml" ps --status running --services \
      | awk -v svc="$service" '$0 == svc { found = 1 } END { exit !found }'; then
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "Timed out waiting for service: $service" >&2
      return 1
    fi
  done
}

init_hbase_table() {
  local hbase_cmd='echo "create '\'''"$TABLE"''\'', '\'''"$CF"''\''" | hbase shell -n'
  if docker compose -f "$ROOT/docker-compose.yml" exec -T hbase bash -lc "command -v hbase >/dev/null 2>&1"; then
    docker compose -f "$ROOT/docker-compose.yml" exec -T hbase bash -lc "$hbase_cmd" || true
    return
  fi

  if docker compose -f "$ROOT/docker-compose.yml" exec -T hbase bash -lc "[ -x /hbase/bin/hbase ]"; then
    docker compose -f "$ROOT/docker-compose.yml" exec -T hbase bash -lc "echo \"create '$TABLE', '$CF'\" | /hbase/bin/hbase shell -n" || true
    return
  fi

  if docker compose -f "$ROOT/docker-compose.yml" exec -T hbase bash -lc "[ -x /opt/hbase/bin/hbase ]"; then
    docker compose -f "$ROOT/docker-compose.yml" exec -T hbase bash -lc "echo \"create '$TABLE', '$CF'\" | /opt/hbase/bin/hbase shell -n" || true
    return
  fi

  echo "Warning: could not find HBase shell in container; skip auto table creation."
  echo "Please create table manually: create '$TABLE', '$CF'"
}

run_setup() {
  echo "1) Starting Kafka + HBase containers..."
  docker compose -f "$ROOT/docker-compose.yml" up -d
  wait_for_service kafka 60
  wait_for_service hbase 60

  echo "2) Creating Kafka topic '$TOPIC'..."
  KAFKA_TOPIC="$TOPIC" KAFKA_PARTITIONS="$PARTS" "$ROOT/scripts/create-kafka-topic.sh"

  echo "3) Creating HBase table '$TABLE' (if needed)..."
  init_hbase_table

  echo "4) Building Java modules..."
  (cd "$ROOT" && mvn -q package -DskipTests)

  echo "5) Preparing dashboard virtualenv..."
  if [ ! -d "$ROOT/dashboard/.venv" ]; then
    python3 -m venv "$ROOT/dashboard/.venv"
  fi
  # shellcheck disable=SC1091
  source "$ROOT/dashboard/.venv/bin/activate"
  pip install -q -r "$ROOT/dashboard/requirements.txt"
  deactivate

  echo "Setup finished (Docker, topic, table, jars, dashboard venv)."
}

ensure_spark() {
  if [ -z "${SPARK_HOME:-}" ]; then
    if [ -d "$ROOT/.local/spark-3.5.1-bin-hadoop3" ]; then
      export SPARK_HOME="$ROOT/.local/spark-3.5.1-bin-hadoop3"
      echo "SPARK_HOME not set; using local Spark at $SPARK_HOME"
    else
      echo "SPARK_HOME is not set." >&2
      echo "Set it and rerun, e.g.: export SPARK_HOME=/path/to/spark-3.5.x-bin-hadoop3" >&2
      exit 1
    fi
  fi
}

export_dashboard_env() {
  if [ -z "${HBASE_THRIFT_HOST:-}" ]; then
    export HBASE_THRIFT_HOST="127.0.0.1"
  fi
  if [ -z "${HBASE_THRIFT_PORT:-}" ]; then
    export HBASE_THRIFT_PORT="9090"
  fi
  if [ -z "${HBASE_THRIFT_TIMEOUT_MS:-}" ]; then
    export HBASE_THRIFT_TIMEOUT_MS="60000"
  fi
  if [ -z "${HBASE_THRIFT_RETRIES:-}" ]; then
    export HBASE_THRIFT_RETRIES="3"
  fi
  if [ -z "${HBASE_THRIFT_RETRY_SLEEP_SECONDS:-}" ]; then
    export HBASE_THRIFT_RETRY_SLEEP_SECONDS="2"
  fi
  if [ -z "${HBASE_THRIFT_HOSTS:-}" ]; then
    export HBASE_THRIFT_HOSTS="hbase,127.0.0.1"
  fi
}

run_apps() {
  ensure_spark
  export_dashboard_env

  echo "6) Starting producer, Spark, and dashboard in background..."
  echo "Dashboard HBase Thrift: host=$HBASE_THRIFT_HOST port=$HBASE_THRIFT_PORT hosts=$HBASE_THRIFT_HOSTS timeout_ms=$HBASE_THRIFT_TIMEOUT_MS retries=$HBASE_THRIFT_RETRIES"
  chmod +x "$ROOT/scripts/spark-submit-local.sh"
  nohup java -jar "$ROOT/ingestion/target/ingestion-1.0.0-SNAPSHOT.jar" > "$LOG_DIR/producer.log" 2>&1 &
  PRODUCER_PID=$!

  nohup "$ROOT/scripts/spark-submit-local.sh" > "$LOG_DIR/spark.log" 2>&1 &
  SPARK_PID=$!

  nohup "$ROOT/dashboard/.venv/bin/streamlit" run "$ROOT/dashboard/app.py" \
    --server.headless true \
    --browser.gatherUsageStats false \
    > "$LOG_DIR/dashboard.log" 2>&1 &
  DASHBOARD_PID=$!

  echo
  echo "Project started."
  echo "Producer PID : $PRODUCER_PID"
  echo "Spark PID    : $SPARK_PID"
  echo "Dashboard PID: $DASHBOARD_PID"
  echo
  echo "Dashboard URL: http://localhost:8501"
  echo "Logs:"
  echo "  $LOG_DIR/producer.log"
  echo "  $LOG_DIR/spark.log"
  echo "  $LOG_DIR/dashboard.log"
  echo
  echo "To stop everything quickly:"
  echo "  $(basename "$0" | sed 's/run-all/setup-and-run/') stop"
  echo "  or: pkill -f 'ingestion-1.0.0-SNAPSHOT.jar' || true"
  echo "       pkill -f 'StreamingPipelineApp' || true"
  echo "       pkill -f 'streamlit run' || true"
  echo "       docker compose -f \"$ROOT/docker-compose.yml\" down"
}

case "$MODE" in
  help|-h|--help)
    usage
    exit 0
    ;;
  setup)
    run_setup
    ;;
  run)
    run_apps
    ;;
  all)
    run_setup
    run_apps
    ;;
esac
