package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class JsonEventResponseHandler extends AbstractEventsResponseHandler<JsonNode> {

    public JsonEventResponseHandler(final EventTypePartition eventTypePartition, final ObjectMapper jsonMapper,
            final PartitionCoordinator coordinator, final JsonEventHandler delegate) {
        super(eventTypePartition, coordinator, JsonEventResponseHandler.class, jsonMapper, delegate);
    }

    @Override
    NakadiEventBatch<JsonNode> getEventBatch(final String string) {
        return EventUtils.getJsonEventBatch(jsonMapper, string);
    }
}
