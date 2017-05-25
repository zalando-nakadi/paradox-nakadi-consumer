package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class RawEventResponseHandler extends AbstractEventsResponseHandler<String> {

    public RawEventResponseHandler(final String consumerName, final EventTypePartition eventTypePartition,
            final ObjectMapper jsonMapper, final PartitionCoordinator coordinator, final RawEventHandler delegate) {
        super(consumerName, eventTypePartition, coordinator, RawEventResponseHandler.class, jsonMapper, delegate);
    }

    @Override
    NakadiEventBatch<String> getEventBatch(final String string) {
        return EventUtils.getRawEventBatch(jsonMapper, string);
    }
}
