package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.io.IOException;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class BatchEventsResponseBulkHandler<T> extends AbstractEventsResponseBulkHandler<T> {
    private final JavaType javaType;

    public BatchEventsResponseBulkHandler(final EventTypePartition eventTypePartition, final ObjectMapper jsonMapper,
            final PartitionCoordinator coordinator, final BatchEventsBulkHandler<T> delegate) {
        super(eventTypePartition, coordinator, BatchEventsResponseBulkHandler.class, jsonMapper, delegate);
        this.javaType = EventClassProvider.getJavaType(delegate, jsonMapper);
    }

    @Override
    NakadiEventBatch<T> getEventBatch(final String string) {
        try {
            return jsonMapper.readValue(string, javaType);
        } catch (final IOException e) {
            log.error("Error while parsing event batch from [{}]", string, e);
            ThrowableUtils.throwException(e);
            return null;
        }
    }
}
