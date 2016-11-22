package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import static java.util.Objects.requireNonNull;

import java.util.List;

import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.LoggingUtils;

abstract class AbstractEventsResponseHandler<T> extends AbstractResponseHandler {

    private final EventHandler<T> delegate;

    AbstractEventsResponseHandler(final EventTypePartition eventTypePartition, final PartitionCoordinator coordinator,
            final Class<?> loggerClazz, final ObjectMapper jsonMapper, final EventHandler<T> delegate) {
        super(eventTypePartition, coordinator, LoggingUtils.getLogger(loggerClazz, eventTypePartition), jsonMapper);
        this.delegate = delegate;
    }

    @Override
    public void onResponse(final String content) {
        final String[] events = getEvents(content);
        for (final String event : events) {

            final NakadiEventBatch<T> nakadiEventBatch = requireNonNull(getNakadiEventBatch(event),
                    "Nakadi event batch must not be null!");

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

    @Nullable
    private NakadiEventBatch<T> getNakadiEventBatch(final String event) {
        try {
            return getEventBatch(event);
        } catch (final Throwable t) {
            coordinator.error(t, eventTypePartition, null, event);
            return null;
        }
    }

    private void handleEvents(final EventTypeCursor cursor, final List<T> events, final String content) {
        for (final T event : events) {
            try {
                delegate.onEvent(cursor, event);
            } catch (final Throwable t) {
                log.error("Handler error at cursor [{}]", cursor);
                coordinator.error(t, eventTypePartition, cursor.getOffset(), content);
            }
        }

        coordinator.commit(cursor);
    }

    abstract NakadiEventBatch<T> getEventBatch(final String string);
}
