package org.example.procesor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.io.IOException;
import java.util.Map;

public class BotCountStreamBuilder {

    private static final String BOT_COUNT_STORE = "bot-count-store";
    private static final String BOT_COUNT_TOPIC = "wikimedia.stats.bots";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KStream<String, String> inputStream;

    public BotCountStreamBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void setup() {
        this.inputStream
                .mapValues(changeJson -> {
                    try {
                        JsonNode node = OBJECT_MAPPER.readTree(changeJson);
                        return node.path("bot").asBoolean(false) ? "bot" : "non-bot";
                    } catch (Exception e) {
                        return "parse-error";
                    }
                })
                .filter((k, v) -> v != null)
                .selectKey((k, v) -> v)
                .groupByKey()
                .count(Materialized.as(BOT_COUNT_STORE))
                .toStream()
                .mapValues((key, value) ->
                        {
                            try {
                                return OBJECT_MAPPER.writeValueAsString(Map.of(key, value));
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        }
                )
                .to(BOT_COUNT_TOPIC);

    }
}
