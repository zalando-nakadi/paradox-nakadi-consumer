package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import de.zalando.paradox.nakadi.consumer.core.domain.NakadiCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

class EventUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventUtils.class);

    private EventUtils() { }

    static Optional<NakadiEventBatch<String>> getRawEventBatch(final ObjectMapper jsonMapper, final String string) {
        try {
            final EventReader reader = new EventReader(jsonMapper, string).invoke();
            final JsonNode eventsNode = reader.getEventsNode();

            final List<String> rawEvents;
            if (null != eventsNode && eventsNode.isArray() && eventsNode.size() > 0) {
                rawEvents = new ArrayList<>(eventsNode.size());

                final ArrayNode arrayNode = (ArrayNode) eventsNode;
                arrayNode.elements().forEachRemaining(element -> {
                    if (!element.isNull()) {
                        String rawEvent = null;
                        try {
                            rawEvent = jsonMapper.writeValueAsString(element);
                        } catch (JsonProcessingException e) {
                            ThrowableUtils.throwException(e);
                        }

                        // back to string -> better solution should be provided
                        if (StringUtils.isNotEmpty(rawEvent)) {
                            rawEvents.add(rawEvent);
                        }
                    }
                });
            } else {
                rawEvents = Collections.emptyList();
            }

            return Optional.of(new NakadiEventBatch<>(new NakadiCursor(reader.getPartition(), reader.getOffset()),
                        rawEvents));
        } catch (IOException e) {
            LOGGER.error("Error while parsing event batch from [{}]", string, e);
            return Optional.empty();
        }
    }

    static Optional<NakadiEventBatch<JsonNode>> getJsonEventBatch(final ObjectMapper jsonMapper, final String string) {
        try {
            final EventReader reader = new EventReader(jsonMapper, string).invoke();
            final JsonNode eventsNode = reader.getEventsNode();

            final List<JsonNode> jsonEvents;
            if (null != eventsNode && eventsNode.isArray() && eventsNode.size() > 0) {
                jsonEvents = new ArrayList<>(eventsNode.size());

                final ArrayNode arrayNode = (ArrayNode) eventsNode;
                arrayNode.elements().forEachRemaining(element -> {
                    if (!element.isNull()) {
                        jsonEvents.add(element);
                    }
                });
            } else {
                jsonEvents = Collections.emptyList();
            }

            return Optional.of(new NakadiEventBatch<>(new NakadiCursor(reader.getPartition(), reader.getOffset()),
                        jsonEvents));
        } catch (IOException e) {
            LOGGER.error("Error while parsing event json from [{}]", string, e);
            return Optional.empty();
        }
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
