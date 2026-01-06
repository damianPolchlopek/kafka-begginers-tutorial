package org.example.procesor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class BotCountStreamBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(BotCountStreamBuilder.class);
    private static final String BOT_COUNT_STORE = "bot-count-store";
    private static final String BOT_COUNT_TOPIC = "wikimedia.stats.bots";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KStream<String, String> inputStream;

    public BotCountStreamBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void setup() {
        this.inputStream
                .mapValues((readOnlyKey, changeJson) -> {
                    if (changeJson == null || changeJson.isBlank()) {
                        LOGGER.warn("Received null or empty message");
                        return null;
                    }
                    try {
                        JsonNode node = OBJECT_MAPPER.readTree(changeJson);
                        boolean isBot = node.path("bot").asBoolean(false);
                        return isBot ? "bot" : "non-bot";
                    } catch (Exception e) {
                        LOGGER.error("Error parsing JSON: {}", changeJson, e);
                        return null;
                    }
                })
                .filter((k, v) -> v != null)
                .selectKey((k, v) -> v)
                .groupByKey()
                .count(Materialized.as(BOT_COUNT_STORE))
                .toStream()
                .mapValues((key, value) -> {
                    try {
                        return OBJECT_MAPPER.writeValueAsString(
                                Map.of(
                                        "type", key,
                                        "count", value
                                )
                        );
                    } catch (JsonProcessingException e) {
                        LOGGER.error("Error serializing bot count result for key: {}", key, e);
                        return null;
                    }
                })
                .filter((k, v) -> v != null)
                .to(BOT_COUNT_TOPIC);
    }
}