package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class EventUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventUtils.class);

    private EventUtils() { }

    public static NakadiEventBatch<String> getRawEventBatch(final ObjectMapper jsonMapper, final String string,
            final EventType eventType) {
        try {
            final EventReader reader = new EventReader(jsonMapper, string).invoke();
            final JsonNode eventsNode = reader.getEventsNode();

            final List<String> rawEvents;
            if (null != eventsNode && eventsNode.isArray() && eventsNode.size() > 0) {
                rawEvents = new ArrayList<>(eventsNode.size());

                final ArrayNode arrayNode = (ArrayNode) eventsNode;
                arrayNode.elements().forEachRemaining(element -> {
                    try {
                        String rawEvent = null;
                        if (isEventOfValidType(element, eventType)) {
                            rawEvent = jsonMapper.writeValueAsString(element);
                        }

                        // back to string -> better solution should be provided
                        if (StringUtils.isNotEmpty(rawEvent)) {
                            rawEvents.add(rawEvent);
                        }
                    } catch (JsonProcessingException e) {
                        ThrowableUtils.throwException(e);
                    }

                });
            } else {
                rawEvents = Collections.emptyList();
            }

            return new NakadiEventBatch<>(new NakadiCursor(reader.getPartition(), reader.getOffset()), rawEvents);
        } catch (IOException e) {
            LOGGER.error("Error while parsing event batch from [{}]", string, e);
            ThrowableUtils.throwException(e);
            return null;
        }
    }

    public static NakadiEventBatch<JsonNode> getJsonEventBatch(final ObjectMapper jsonMapper, final String string,
            final EventType eventType) {
        try {
            final EventReader reader = new EventReader(jsonMapper, string).invoke();
            final JsonNode eventsNode = reader.getEventsNode();

            final List<JsonNode> jsonEvents;
            if (null != eventsNode && eventsNode.isArray() && eventsNode.size() > 0) {
                jsonEvents = new ArrayList<>(eventsNode.size());

                final ArrayNode arrayNode = (ArrayNode) eventsNode;
                arrayNode.elements().forEachRemaining(element -> {
                    if (isEventOfValidType(element, eventType)) {
                        jsonEvents.add(element);
                    }
                });
            } else {
                jsonEvents = Collections.emptyList();
            }

            return new NakadiEventBatch<>(new NakadiCursor(reader.getPartition(), reader.getOffset()), jsonEvents);
        } catch (IOException e) {
            LOGGER.error("Error while parsing event json from [{}]", string, e);
            ThrowableUtils.throwException(e);
            return null;
        }
    }

    private static boolean isEventOfValidType(final JsonNode element, final EventType eventType) {
        if (!element.isNull() && element.has("metadata")) {
            if (!element.get("metadata").has("event_type")
                    || element.get("metadata").get("event_type").asText().equals(eventType.getName())) {
                return true;
            } else {
                LOGGER.warn("Unexpected event type (expected=[{}], actual=[{}])", eventType.getName(),
                    element.get("metadata").get("event_type").asText());
            }
        }

        return false;
    }

    private static class EventReader {
        private ObjectMapper jsonMapper;
        private String string;
        private String partition;
        private String offset;
        private JsonNode eventsNode;

        EventReader(final ObjectMapper jsonMapper, final String string) {
            this.jsonMapper = jsonMapper;
            this.string = string;
        }

        String getPartition() {
            return partition;
        }

        String getOffset() {
            return offset;
        }

        JsonNode getEventsNode() {
            return eventsNode;
        }

        EventReader invoke() throws IOException {
            final JsonNode node = jsonMapper.readTree(string);
            final JsonNode cursorNode = requireNonNull(node.get("cursor"), "cursor node must not be null");

            partition = cursorNode.get("partition").textValue();
            checkArgument(StringUtils.isNotEmpty(partition), "cursor.partition must not be empty");

            offset = cursorNode.get("offset").textValue();
            checkArgument(StringUtils.isNotEmpty(offset), "cursor.offset must not be empty");

            // optional for keep alive
            eventsNode = node.get("events");
            return this;
        }
    }
}
