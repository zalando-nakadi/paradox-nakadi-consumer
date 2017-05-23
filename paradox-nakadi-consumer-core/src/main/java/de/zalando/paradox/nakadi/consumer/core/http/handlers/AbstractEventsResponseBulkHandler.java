package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.LoggingUtils;

abstract class AbstractEventsResponseBulkHandler<T> extends AbstractResponseHandler {

    private final EventHandler<List<T>> delegate;

    AbstractEventsResponseBulkHandler(final String consumerName, final EventTypePartition eventTypePartition,
            final PartitionCoordinator coordinator, final Class<?> loggerClazz, final ObjectMapper jsonMapper,
            final EventHandler<List<T>> delegate) {
        super(consumerName, eventTypePartition, coordinator, LoggingUtils.getLogger(loggerClazz, eventTypePartition),
            jsonMapper);
        this.delegate = delegate;
    }

    @Override
    public void onResponse(final String content) {
        final String[] events = getEvents(content);
        for (final String event : events) {
            final NakadiEventBatch<T> nakadiEventBatch = requireNonNull(getEventBatch(event),
                    "nakadiEventBatch must not be null");

            final EventTypeCursor cursor = EventTypeCursor.of(eventTypePartition,
                    nakadiEventBatch.getCursor().getOffset());

            final List<T> batchEvents = nakadiEventBatch.getEvents();

            if (batchEvents == null || batchEvents.isEmpty()) {
                log.info("Keep alive offset [{}]", cursor.getOffset());
            } else {
                handleEvents(cursor, batchEvents, content);
            }
        }
    }

    private void handleEvents(final EventTypeCursor cursor, final List<T> events, final String content) {
        try {
            delegate.onEvent(cursor, events);
        } catch (final Throwable t) {
            log.error("Handler error at cursor [{}]", cursor);
            coordinator.error(consumerName, t, eventTypePartition, cursor.getOffset(), content);
        }

        coordinator.commit(cursor);
    }

    abstract NakadiEventBatch<T> getEventBatch(final String string);
}
