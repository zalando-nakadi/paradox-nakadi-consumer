package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class JsonEventResponseBulkHandler extends AbstractEventsResponseBulkHandler<JsonNode> {

    public JsonEventResponseBulkHandler(final EventTypePartition eventTypePartition, final ObjectMapper jsonMapper,
            final PartitionCoordinator coordinator, final JsonEventBulkHandler delegate) {
        super(eventTypePartition, coordinator, JsonEventResponseBulkHandler.class, jsonMapper, delegate);
    }

    @Override
    NakadiEventBatch<JsonNode> getEventBatch(final String string) {
        return EventUtils.getJsonEventBatch(jsonMapper, string);
    }
}
