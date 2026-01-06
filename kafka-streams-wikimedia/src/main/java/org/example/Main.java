package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.example.procesor.BotCountStreamBuilder;
import org.example.procesor.EventCountTimeseriesBuilder;
import org.example.procesor.WebsiteCountStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {
        private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
        private static final String INPUT_TOPIC = "wikimedia.recentchange";

    public static void main(String[] args) {
        Properties properties = buildProperties();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> changeJsonStream = builder.stream(INPUT_TOPIC);

        // Setup all processors
        new BotCountStreamBuilder(changeJsonStream).setup();
        new WebsiteCountStreamBuilder(changeJsonStream).setup();
        new EventCountTimeseriesBuilder(changeJsonStream).setup();

        final Topology appTopology = builder.build();
        LOGGER.info("Topology: {}", appTopology.describe());

        final KafkaStreams streams = new KafkaStreams(appTopology, properties);
        final CountDownLatch latch = new CountDownLatch(1);

        // Set exception handler
        streams.setUncaughtExceptionHandler(exception -> {
            LOGGER.error("Kafka Streams uncaught exception occurred. Stream will be replaced.", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                LOGGER.info("Shutdown hook triggered. Closing streams...");
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            LOGGER.info("Kafka Streams application started successfully");
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Application interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            streams.close();
            LOGGER.info("Application closed");
        }
    }

    private static Properties buildProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-stats-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Performance tuning
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L); // 10MB
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // 1 second

        // Error handling
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class);

        return properties;
    }
}
