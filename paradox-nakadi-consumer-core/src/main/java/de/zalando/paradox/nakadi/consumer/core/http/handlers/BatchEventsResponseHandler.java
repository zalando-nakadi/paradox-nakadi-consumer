package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.io.IOException;

import java.util.Optional;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class BatchEventsResponseHandler<T> extends AbstractEventsResponseHandler<T> {
    private final JavaType javaType;

    public BatchEventsResponseHandler(final EventTypePartition eventTypePartition, final ObjectMapper jsonMapper,
            final PartitionCoordinator coordinator, final BatchEventsHandler<T> delegate) {
        super(eventTypePartition, coordinator, BatchEventsResponseHandler.class, jsonMapper, delegate);
        this.javaType = EventClassProvider.getJavaType(delegate, jsonMapper);
    }

    @Override
    Optional<NakadiEventBatch<T>> getEventBatch(final String string) {
        try {
            return Optional.of(jsonMapper.readValue(string, javaType));
        } catch (IOException e) {
            log.error("Error while parsing event batch from [{}]", string, e);
            return Optional.empty();
        }
    }
}
