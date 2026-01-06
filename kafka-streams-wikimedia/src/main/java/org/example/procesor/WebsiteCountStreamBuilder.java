package org.example.procesor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

public class WebsiteCountStreamBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebsiteCountStreamBuilder.class);
    private static final String WEBSITE_COUNT_STORE = "website-count-store";
    private static final String WEBSITE_COUNT_TOPIC = "wikimedia.stats.website";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KStream<String, String> inputStream;

    public WebsiteCountStreamBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void setup() {
        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L));

        this.inputStream
                .selectKey((k, changeJson) -> {
                    if (changeJson == null || changeJson.isBlank()) {
                        LOGGER.warn("Received null or empty message");
                        return "unknown";
                    }
                    try {
                        final JsonNode jsonNode = OBJECT_MAPPER.readTree(changeJson);
                        JsonNode serverNameNode = jsonNode.get("server_name");
                        if (serverNameNode == null || serverNameNode.isNull()) {
                            LOGGER.warn("Missing server_name in message: {}", changeJson);
                            return "unknown";
                        }
                        return serverNameNode.asText();
                    } catch (Exception e) {
                        LOGGER.error("Error parsing JSON for website: {}", changeJson, e);
                        return "parse-error";
                    }
                })
                .filter((k, v) -> k != null && !k.equals("unknown") && !k.equals("parse-error"))
                .groupByKey()
                .windowedBy(timeWindows)
                .count(Materialized.as(WEBSITE_COUNT_STORE))
                .toStream()
                .mapValues((key, value) -> {
                    try {
                        return OBJECT_MAPPER.writeValueAsString(
                                Map.of(
                                        "website", key.key(),
                                        "count", value,
                                        "window_start", key.window().startTime().toString(),
                                        "window_end", key.window().endTime().toString()
                                )
                        );
                    } catch (JsonProcessingException e) {
                        LOGGER.error("Error serializing website count result for key: {}", key, e);
                        return null;
                    }
                })
                .filter((k, v) -> v != null)
                .to(WEBSITE_COUNT_TOPIC, Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, timeWindows.size()),
                        Serdes.String()
                ));
    }
}