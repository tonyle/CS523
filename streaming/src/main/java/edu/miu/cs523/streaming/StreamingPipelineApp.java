package edu.miu.cs523.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.window;

public final class StreamingPipelineApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingPipelineApp.class);
    private static final byte[] CF = Bytes.toBytes("m");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("KAFKA_TOPIC", "crypto-trades");
        String checkpoint = env("CHECKPOINT_DIR", "./checkpoints/streaming");
        String metadataCsv = env("METADATA_CSV", "data/crypto_metadata.csv");
        String hbaseTable = env("HBASE_TABLE", "crypto_stream_metrics");
        String zkQuorum = env("HBASE_ZOOKEEPER_QUORUM", "localhost");
        String zkPort = env("HBASE_ZOOKEEPER_CLIENT_PORT", "2181");
        boolean skipEnrichment = Boolean.parseBoolean(env("SKIP_METADATA_JOIN", "false"));

        SparkSession spark = SparkSession.builder()
                .appName("cs523-crypto-streaming")
                .master(env("SPARK_MASTER", "local[*]"))
                .config("spark.sql.shuffle.partitions", env("SHUFFLE_PARTITIONS", "8"))
                .config("spark.sql.caseSensitive", "true")
                .getOrCreate();

        StructType tradeSchema = new StructType(new StructField[]{
                DataTypes.createStructField("e", DataTypes.StringType, true),
                DataTypes.createStructField("E", DataTypes.LongType, true),
                DataTypes.createStructField("s", DataTypes.StringType, true),
                DataTypes.createStructField("p", DataTypes.StringType, true),
                DataTypes.createStructField("q", DataTypes.StringType, true),
        });

        Dataset<Row> metadata = null;
        if (!skipEnrichment && metadataCsv != null && !metadataCsv.isBlank()) {
            LOG.info("Loading metadata enrichment CSV from {}", metadataCsv);
            metadata = spark.read()
                    .option("header", true)
                    .option("inferSchema", true)
                    .csv(metadataCsv)
                    .select("symbol", "asset_name", "risk_tier")
                    .cache();
            LOG.info("Loaded {} metadata rows from {}", metadata.count(), metadataCsv);
        } else {
            LOG.info("Skipping metadata enrichment. SKIP_METADATA_JOIN={}, METADATA_CSV={}", skipEnrichment, metadataCsv);
        }

        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> trades = raw.select(col("value").cast("string").alias("json"))
                .select(from_json(col("json"), tradeSchema).alias("t"))
                .select("t.*")
                .filter(col("e").equalTo(lit("trade")))
                .filter(col("s").isNotNull())
                .filter(col("p").cast("double").geq(lit(0.0)))
                .withColumn("price", col("p").cast("double"))
                .withColumn("qty", col("q").cast("double"))
                .withColumn("event_time", to_timestamp(col("E").divide(lit(1000L))))
                .filter(col("event_time").isNotNull());

        Dataset<Row> withInsights = trades
                .withWatermark("event_time", env("WATERMARK_DELAY", "2 minutes"))
                .groupBy(
                        window(col("event_time"), env("WINDOW_DURATION", "30 seconds"), env("WINDOW_SLIDE", "10 seconds")),
                        col("s")
                )
                .agg(
                        avg(col("price")).alias("avg_price"),
                        min(col("price")).alias("min_price"),
                        max(col("price")).alias("max_price"),
                        count(lit(1)).alias("trade_count")
                )
                .withColumn(
                        "price_range_pct",
                        when(
                                col("avg_price").gt(lit(0.0)),
                                col("max_price").minus(col("min_price")).divide(col("avg_price")).multiply(lit(100.0))
                        ).otherwise(lit(0.0))
                )
                .withColumn(
                        "anomaly",
                        col("price_range_pct").gt(lit(Double.parseDouble(env("ANOMALY_THRESHOLD_PCT", "0.3"))))
                );

        Dataset<Row> enriched;
        if (metadata != null) {
            enriched = withInsights.join(
                    metadata,
                    withInsights.col("s").equalTo(metadata.col("symbol")),
                    "left_outer"
            ).drop(metadata.col("symbol"));
        } else {
            enriched = withInsights;
        }

        VoidFunction2<Dataset<Row>, Long> hbaseWriter = (batchDF, batchId) -> {
            long batchRows = batchDF.count();
            System.out.println("HBase sink batch " + batchId + " rows=" + batchRows);
            if (batchRows == 0L) {
                return;
            }
            VoidFunction<java.util.Iterator<Row>> partitionWriter = iter -> {
                Configuration hconf = HBaseConfiguration.create();
                hconf.set("hbase.zookeeper.quorum", zkQuorum);
                hconf.set("hbase.zookeeper.property.clientPort", zkPort);
                hconf.set("zookeeper.znode.parent", env("HBASE_ZNODE_PARENT", "/hbase"));
                try (Connection connection = ConnectionFactory.createConnection(hconf)) {
                    try (Table table = connection.getTable(TableName.valueOf(hbaseTable))) {
                        while (iter.hasNext()) {
                            Row r = iter.next();
                            putRow(table, r);
                        }
                    }
                }
            };
            batchDF.javaRDD().foreachPartition(partitionWriter);
        };

        enriched.writeStream()
                .queryName("cs523_crypto_to_hbase")
                .outputMode("update")
                .option("checkpointLocation", checkpoint)
                .foreachBatch(hbaseWriter)
                .start()
                .awaitTermination();
    }

    private static void putRow(Table table, Row row) throws Exception {
        Timestamp end;
        if (hasColumn(row, "window")) {
            Row win = row.getStruct(row.fieldIndex("window"));
            end = win.getTimestamp(1);
        } else {
            end = row.getAs("event_time");
        }
        String symbol = row.getString(row.fieldIndex("s"));
        long endMs = end.getTime();
        String rowKey = symbol + "#" + endMs;
        Put put = new Put(Bytes.toBytes(rowKey));
        double avgPx = row.getAs("avg_price");
        long trades = row.getAs("trade_count");
        double minP = row.getAs("min_price");
        double maxP = row.getAs("max_price");
        double rangePct = row.getAs("price_range_pct");
        boolean anomaly = row.getAs("anomaly");
        put.addColumn(CF, Bytes.toBytes("symbol"), Bytes.toBytes(symbol));
        put.addColumn(CF, Bytes.toBytes("window_end_ms"), Bytes.toBytes(String.valueOf(endMs)));
        put.addColumn(CF, Bytes.toBytes("avg_price"), Bytes.toBytes(String.valueOf(avgPx)));
        put.addColumn(CF, Bytes.toBytes("trade_count"), Bytes.toBytes(String.valueOf(trades)));
        put.addColumn(CF, Bytes.toBytes("min_price"), Bytes.toBytes(String.valueOf(minP)));
        put.addColumn(CF, Bytes.toBytes("max_price"), Bytes.toBytes(String.valueOf(maxP)));
        put.addColumn(CF, Bytes.toBytes("price_range_pct"), Bytes.toBytes(String.valueOf(rangePct)));
        put.addColumn(CF, Bytes.toBytes("anomaly"), Bytes.toBytes(Boolean.toString(anomaly)));
        if (hasColumn(row, "asset_name")) {
            Object v = row.getAs("asset_name");
            if (v != null) {
                put.addColumn(CF, Bytes.toBytes("asset_name"), Bytes.toBytes(v.toString()));
            }
        }
        if (hasColumn(row, "risk_tier")) {
            Object v = row.getAs("risk_tier");
            if (v != null) {
                put.addColumn(CF, Bytes.toBytes("risk_tier"), Bytes.toBytes(v.toString()));
            }
        }
        table.put(put);
    }

    private static boolean hasColumn(Row row, String name) {
        try {
            row.fieldIndex(name);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v;
    }
}
