package edu.miu.cs523.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.http.HttpClient;
import java.net.http.WebSocket;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Phase 1: Decoupled real-time ingestion from Binance public WebSocket into Kafka.
 * Uses {@link KafkaProducer} only; Spark consumers read independently.
 */
public final class CryptoTradeKafkaProducer {

    private static final Logger LOG = LoggerFactory.getLogger(CryptoTradeKafkaProducer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("KAFKA_TOPIC", "crypto-trades");
        String metadataCsv = env("METADATA_CSV", "data/crypto_metadata.csv");
        String wsUrl = System.getenv("BINANCE_WS_URL");
        if (wsUrl == null || wsUrl.isBlank()) {
            List<String> symbols = loadSymbols(metadataCsv);
            wsUrl = buildCombinedStreamUrl(symbols);
            LOG.info("BINANCE_WS_URL not set; using {} symbols from {}: {}", symbols.size(), metadataCsv, symbols);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        CountDownLatch running = new CountDownLatch(1);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            WebSocketListener listener = new WebSocketListener(producer, topic);
            LOG.info("Connecting Binance WebSocket: {}", wsUrl);
            WebSocket ws = HttpClient.newHttpClient()
                    .newWebSocketBuilder()
                    .buildAsync(URI.create(wsUrl), listener)
                    .join();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown")
                            .get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOG.warn("WebSocket close: {}", e.toString());
                }
                running.countDown();
            }));

            running.await();
        }
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v;
    }

    private static List<String> loadSymbols(String metadataCsv) throws Exception {
        List<String> symbols = Files.lines(Path.of(metadataCsv))
                .skip(1)
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .map(line -> line.split(",", -1)[0].trim())
                .filter(symbol -> !symbol.isEmpty())
                .distinct()
                .collect(Collectors.toList());
        if (symbols.isEmpty()) {
            throw new IllegalArgumentException("No symbols found in metadata CSV: " + metadataCsv);
        }
        return symbols;
    }

    private static String buildCombinedStreamUrl(List<String> symbols) {
        String streams = symbols.stream()
                .map(symbol -> symbol.toLowerCase(Locale.ROOT) + "@trade")
                .collect(Collectors.joining("/"));
        return "wss://stream.binance.us:9443/stream?streams=" + streams;
    }

    static final class WebSocketListener implements WebSocket.Listener {
        private final KafkaProducer<String, String> producer;
        private final String topic;
        private final StringBuilder buffer = new StringBuilder();
        private int payloadSamplesLogged = 0;

        WebSocketListener(KafkaProducer<String, String> producer, String topic) {
            this.producer = producer;
            this.topic = topic;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            LOG.info("Binance WebSocket connected; streaming to topic {}", topic);
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String payload = buffer.toString();
                buffer.setLength(0);
                if (payloadSamplesLogged < 3) {
                    LOG.info("Sample WebSocket payload: {}", payload);
                    payloadSamplesLogged++;
                }
                try {
                    JsonNode root = MAPPER.readTree(payload);
                    if (root.isArray()) {
                        for (JsonNode node : root) {
                            handleTrade(node);
                        }
                    } else if (root.has("data") && root.get("data").isObject()) {
                        // Binance combined streams wrap payloads as {"stream":"...","data":{...}}.
                        handleTrade(root.get("data"));
                    } else {
                        handleTrade(root);
                    }
                } catch (Exception e) {
                    LOG.debug("Skip non-JSON or parse error: {}", e.toString());
                }
                webSocket.request(1);
            } else {
                webSocket.request(1);
            }
            return CompletableFuture.completedFuture(null);
        }

        private void handleTrade(JsonNode node) {
            if (node == null || !node.has("e") || !"trade".equals(node.get("e").asText())) {
                return;
            }
            String symbol = node.path("s").asText("");
            if (symbol.isEmpty()) {
                return;
            }
            String key = symbol;
            String value = node.toString();
            producer.send(new ProducerRecord<>(topic, key, value), (metadata, err) -> {
                if (err != null) {
                    LOG.error("Kafka send failed: {}", err.toString());
                }
            });
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            LOG.error("WebSocket error", error);
        }
    }
}
