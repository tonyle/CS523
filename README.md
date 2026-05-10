# CS523 Big Data Pipeline (Java)


| Phase | Deliverable |
| --- | --- |
| 1 — Ingestion | Java `KafkaProducer` + Binance WebSocket (`ingestion`) |
| 2 — Streaming | Spark Structured Streaming: watermark, sliding windows, anomaly flag (`streaming`) |
| 3 — Enrichment | Spark SQL join to static CSV (bonus path; `METADATA_CSV`) |
| 4 — Storage | Micro-batch writes to Apache HBase |
| 5 — Visualization | Streamlit dashboard reading HBase over Thrift (`dashboard`) |

Prerequisites:

- JDK 11+, Apache Maven 3.8+, Apache Spark 3.5.x (Scala 2.12 build), Docker (Compose v2).

## 1. Start Kafka and HBase

From this directory:

```bash
docker compose up -d
```

Create the Kafka topic:

```bash
chmod +x scripts/create-kafka-topic.sh
./scripts/create-kafka-topic.sh
```

Create the HBase table (after HBase answers on port `16010`; adjust shell path if your image differs):

```bash
docker compose exec hbase bash -c 'echo "create '\''crypto_stream_metrics'\'', '\''m'\''" | hbase shell -n'
```

If the shell is not available in the container, open an interactive shell (`docker compose exec hbase bash`) and run `hbase shell`, then paste the command from [`scripts/hbase-init.txt`](scripts/hbase-init.txt).

## 2. Build Java modules

```bash
mvn -q package -DskipTests
```

- Producer uber-jar: `ingestion/target/ingestion-1.0.0-SNAPSHOT.jar`
- Streaming app jar (classes only; Spark resolves deps): `streaming/target/streaming-1.0.0-SNAPSHOT.jar`

## 3. Run the producer

Streams Binance trades for the symbols in `data/crypto_metadata.csv` to topic `crypto-trades` (configurable):

```bash
java -jar ingestion/target/ingestion-1.0.0-SNAPSHOT.jar
```

Environment (optional):

- `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
- `KAFKA_TOPIC` (default `crypto-trades`)
- `METADATA_CSV` (default `data/crypto_metadata.csv`; producer reads symbols from this file)
- `BINANCE_WS_URL` (optional explicit WebSocket URL; overrides metadata-driven stream creation)

## 4. Run Spark Structured Streaming

Install spark hadoop3 if not ready
```bash
curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark.tgz
```

By default, `scripts/spark-submit-local.sh` reads the enrichment metadata from local HDFS at `hdfs://localhost:9000/user/$USER/cs523/crypto_metadata.csv`. Run `./scripts/setup-local-hdfs-macos.sh` first to install/configure HDFS and upload the CSV. Then submit Spark:

```bash
export SPARK_HOME="$(pwd)/spark-3.5.0-bin-hadoop3"
export CHECKPOINT_DIR="$(pwd)/checkpoints/streaming"
export HBASE_ZOOKEEPER_QUORUM=localhost
export HBASE_ZOOKEEPER_CLIENT_PORT=2181
# Optional analytics tuning:
export WINDOW_DURATION="30 seconds"
export WINDOW_SLIDE="10 seconds"
export WATERMARK_DELAY="2 minutes"
export ANOMALY_THRESHOLD_PCT="0.3"
chmod +x scripts/spark-submit-local.sh
./scripts/spark-submit-local.sh
```

If you want to run without HDFS, override the metadata path with the repo CSV:

```bash
export METADATA_CSV="$(pwd)/data/crypto_metadata.csv"
```

The script now passes Spark/Ivy an explicit repository list so dependency resolution does not rely on the default Maven endpoints:

```bash
--repositories https://repo.maven.apache.org/maven2,https://repos.spark-packages.org
```

If you already have the jars in `~/.m2`, Spark will reuse them; otherwise it will fetch them from the repositories above.

Use `SKIP_METADATA_JOIN=true` to run without the Spark SQL join.

Spark applies:

- Watermarked event time (`E` from Binance, seconds).
- **Sliding windows** (30s length, 10s slide) as a short-horizon moving view; `avg(price)` is the window moving average.
- **Anomaly flag** when intra-window high–low range exceeds 0.3% of the average price.
- **Enrichment** with `data/crypto_metadata.csv` (symbol → asset name, risk tier).

## 5. Dashboard (Streamlit)

Requires HBase Thrift on `localhost:9090` (exposed by the `harisekhon/hbase` image for this stack).

```bash
cd dashboard
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

**If you see “timed out” when reading HBase:** Thrift may still be starting after `docker compose up`. Wait 30–60 seconds and reload the dashboard. Ensure the HBase container is running (`docker compose ps`) and port `9090` is reachable (`nc -zv localhost 9090` or open `http://localhost:16010` for the HBase UI). You can raise the client wait with `export HBASE_THRIFT_TIMEOUT_MS=60000`.

For online/remote deployment, set Thrift endpoint env vars explicitly:

```bash
# same machine as Docker
export HBASE_THRIFT_HOST=127.0.0.1
export HBASE_THRIFT_PORT=9090

# optional retries/fallbacks
export HBASE_THRIFT_RETRIES=3
export HBASE_THRIFT_RETRY_SLEEP_SECONDS=2
export HBASE_THRIFT_HOSTS=hbase,127.0.0.1
```

Notes:

- `localhost` works only when Streamlit and Docker are on the same host.
- If Streamlit runs in Docker, prefer `HBASE_THRIFT_HOST=hbase` (service name on Compose network).
- If Streamlit runs on a different machine, use the Docker host IP/DNS for `HBASE_THRIFT_HOST`.

## 6. HDFS note (bonus)

On macOS, you can install and configure a local single-node HDFS, then upload the metadata CSV, with:

```bash
./scripts/setup-local-hdfs-macos.sh
```

The script prints the `METADATA_CSV` value to use with Spark, for example `hdfs://localhost:9000/user/<you>/cs523/crypto_metadata.csv`. If macOS blocks passwordless `ssh localhost`, the script falls back to direct Hadoop daemon startup automatically.

For cluster submission, place `data/crypto_metadata.csv` on HDFS and set `METADATA_CSV` to `hdfs:///user/<you>/crypto_metadata.csv` with Hadoop configs on the classpath (or `core-site.xml` / `hdfs-site.xml` as usual for Spark):

```bash
hdfs dfs -mkdir -p /user/$USER/cs523
hdfs dfs -put -f data/crypto_metadata.csv /user/$USER/cs523/crypto_metadata.csv
export METADATA_CSV="hdfs:///user/$USER/cs523/crypto_metadata.csv"
```

## Exactly-once semantics

The plan references micro-batches and reliability: HBase here is updated per micro-batch with **idempotent row keys** `symbol#window_end_ms`. End-to-end exactly-once across Kafka → Spark → HBase typically needs Kafka transactions, idempotent producers, and/or HBase MOB/duplicate handling; document your chosen semantics in the course write-up.

## Repository layout

- `ingestion/` — Kafka producer
- `streaming/` — Spark job
- `data/crypto_metadata.csv` — static enrichment
- `dashboard/` — Streamlit UI
- `docker-compose.yml` — Kafka (KRaft) + HBase

## Video demo checklist

Show: public API → Kafka topic → Spark console or logs → HBase `scan` → Streamlit refresh, with all presenters on camera per course requirements.

## Quick Start
Run setup scripts in scripts/ and use docker-compose for local services.
