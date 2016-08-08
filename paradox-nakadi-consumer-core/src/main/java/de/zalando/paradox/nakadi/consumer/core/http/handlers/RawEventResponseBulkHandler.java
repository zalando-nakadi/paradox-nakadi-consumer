package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class RawEventResponseBulkHandler extends AbstractEventsResponseBulkHandler<String> {

    public RawEventResponseBulkHandler(final EventTypePartition eventTypePartition, final ObjectMapper jsonMapper,
            final PartitionCoordinator coordinator, final RawEventBulkHandler delegate) {
        super(eventTypePartition, coordinator, RawEventResponseBulkHandler.class, jsonMapper, delegate);
    }

    @Override
    Optional<NakadiEventBatch<String>> getEventBatch(final String string) {
        return EventUtils.getRawEventBatch(jsonMapper, string);
    }
}
