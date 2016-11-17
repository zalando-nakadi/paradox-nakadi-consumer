package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.LoggingUtils;

abstract class AbstractEventsResponseBulkHandler<T> extends AbstractResponseHandler {

    private final EventHandler<List<T>> delegate;

    AbstractEventsResponseBulkHandler(final EventTypePartition eventTypePartition,
            final PartitionCoordinator coordinator, final Class<?> loggerClazz, final ObjectMapper jsonMapper,
            final EventHandler<List<T>> delegate) {
        super(eventTypePartition, coordinator, LoggingUtils.getLogger(loggerClazz, eventTypePartition), jsonMapper);
        this.delegate = delegate;
    }

    @Override
    public void onResponse(final String content) {
        final String[] events = getEvents(content);
        for (String event : events) {
            final Optional<NakadiEventBatch<T>> optionalBatch = getEventBatch(event);
            if (!optionalBatch.isPresent()) {
                return;
            }

            final EventTypeCursor cursor = EventTypeCursor.of(eventTypePartition,
                    optionalBatch.get().getCursor().getOffset());

            final List<T> batchEvents = optionalBatch.get().getEvents();
            if (null != batchEvents && !batchEvents.isEmpty()) {
                handleEvents(cursor, batchEvents, content);
            } else {
                try {
                    log.info("Keep alive offset [{}]", cursor.getOffset());
                    coordinator.commit(cursor);
                } catch (Throwable t) {
                    log.error("Handler error at cursor [{}]", cursor);
                    coordinator.error(t, eventTypePartition, cursor.getOffset(), content);
                }
            }
        }
    }

    private void handleEvents(final EventTypeCursor cursor, final List<T> events, final String content) {
        try {
            delegate.onEvent(cursor, events);
            coordinator.commit(cursor);
        } catch (final Throwable t) {
            log.error("Handler error at cursor [{}]", cursor);
            coordinator.error(t, eventTypePartition, cursor.getOffset(), content);
        }
    }

    abstract Optional<NakadiEventBatch<T>> getEventBatch(final String string);
}
