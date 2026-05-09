#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/streaming/target/streaming-1.0.0-SNAPSHOT.jar"
META="${METADATA_CSV:-hdfs://localhost:9000/user/${USER:-tonyle}/cs523/crypto_metadata.csv}"
CHECKPOINT="${CHECKPOINT_DIR:-$ROOT/checkpoints/streaming}"
LOCAL_HOSTS_FILE="$ROOT/.local/java-hosts"

SPARK_HOME="${SPARK_HOME:?Set SPARK_HOME to your Spark 3.5 installation}"
export METADATA_CSV="$META"
export CHECKPOINT_DIR="$CHECKPOINT"

SPARK_BIN="$SPARK_HOME/bin/spark-submit"
if [ -z "${JAVA_HOME:-}" ] && command -v /usr/libexec/java_home >/dev/null 2>&1; then
  JAVA_17_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
  if [ -n "$JAVA_17_HOME" ]; then
    export JAVA_HOME="$JAVA_17_HOME"
  fi
fi
SPARK_VERSION_OUT="$("$SPARK_BIN" --version 2>&1 || true)"
SPARK_VERSION_LINE="$(printf '%s\n' "$SPARK_VERSION_OUT" | awk '/version/ {print; exit}')"
if ! printf '%s\n' "$SPARK_VERSION_OUT" | awk '/version/ && /3\.5\./ {ok=1} END{exit !ok}'; then
  echo "Incompatible Spark version. Expected 3.5.x." >&2
  if [ -n "$SPARK_VERSION_LINE" ]; then
    echo "Detected: $SPARK_VERSION_LINE" >&2
  else
    echo "Could not detect Spark version from: $SPARK_BIN --version" >&2
  fi
  exit 1
fi

mkdir -p "$ROOT/.local"
LOCAL_HOSTNAME="$(hostname)"
cat > "$LOCAL_HOSTS_FILE" <<EOF
127.0.0.1 localhost $LOCAL_HOSTNAME hbase
EOF

"$SPARK_BIN" \
  --class edu.miu.cs523.streaming.StreamingPipelineApp \
  --master "${SPARK_MASTER:-local[*]}" \
  --conf "spark.driver.extraJavaOptions=-Djdk.net.hosts.file=$LOCAL_HOSTS_FILE" \
  --conf "spark.executor.extraJavaOptions=-Djdk.net.hosts.file=$LOCAL_HOSTS_FILE" \
  --repositories https://repo.maven.apache.org/maven2,https://repos.spark-packages.org \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hbase:hbase-client:2.4.17,org.apache.hbase:hbase-common:2.4.17 \
  "$JAR"
