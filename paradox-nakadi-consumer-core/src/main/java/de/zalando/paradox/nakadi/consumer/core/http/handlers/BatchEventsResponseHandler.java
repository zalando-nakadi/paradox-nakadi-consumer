package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.io.IOException;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class BatchEventsResponseHandler<T> extends AbstractEventsResponseHandler<T> {
    private final JavaType javaType;

    public BatchEventsResponseHandler(final EventTypePartition eventTypePartition, final ObjectMapper jsonMapper,
            final PartitionCoordinator coordinator, final BatchEventsHandler<T> delegate) {
        super(eventTypePartition, coordinator, BatchEventsResponseHandler.class, jsonMapper, delegate);
        this.javaType = EventClassProvider.getJavaType(delegate, jsonMapper);
    }

    @Override
    NakadiEventBatch<T> getEventBatch(final String string) {
        try {
            return jsonMapper.readValue(string, javaType);
        } catch (final IOException e) {
            ThrowableUtils.throwException(e);
            return null;
        }
    }
}
