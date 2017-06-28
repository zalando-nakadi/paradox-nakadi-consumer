package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class RawEventResponseBulkHandler extends AbstractEventsResponseBulkHandler<String> {

    public RawEventResponseBulkHandler(final String consumerName, final EventTypePartition eventTypePartition,
            final ObjectMapper jsonMapper, final PartitionCoordinator coordinator, final RawEventBulkHandler delegate) {
        super(consumerName, eventTypePartition, coordinator, RawEventResponseBulkHandler.class, jsonMapper, delegate);
    }

    @Override
    NakadiEventBatch<String> getEventBatch(final String string) {
        return EventUtils.getRawEventBatch(jsonMapper, string, eventType);
    }
}
