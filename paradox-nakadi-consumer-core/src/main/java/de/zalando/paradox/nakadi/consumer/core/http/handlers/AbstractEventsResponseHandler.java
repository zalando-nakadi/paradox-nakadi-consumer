package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.LoggingUtils;

abstract class AbstractEventsResponseHandler<T> extends AbstractResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEventsResponseHandler.class);

    private final EventHandler<T> delegate;

    AbstractEventsResponseHandler(final EventTypePartition eventTypePartition, final PartitionCoordinator coordinator,
            final Class<?> loggerClazz, final ObjectMapper jsonMapper, final EventHandler<T> delegate) {
        super(eventTypePartition, coordinator, LoggingUtils.getLogger(loggerClazz, eventTypePartition), jsonMapper);
        this.delegate = delegate;
    }

    @Override
    public void onResponse(final String content) {
        final String[] events = getEvents(content);
        if (events.length > 0) {
            for (String event : events) {
                final Optional<NakadiEventBatch<T>> optionalBatch = getEventBatch(event);
                if (!optionalBatch.isPresent()) {
                    return;
                }

                final EventTypeCursor cursor = EventTypeCursor.of(eventTypePartition,
                        optionalBatch.get().getCursor().getOffset());

                final List<T> batchEvents = optionalBatch.get().getEvents();
                if (null != batchEvents && !batchEvents.isEmpty()) {
                    handleEvents(cursor, batchEvents);
                } else {
                    try {
                        log.info("Keep alive offset [{}]", cursor.getOffset());
                        coordinator.commit(cursor);
                    } catch (Throwable t) {
                        log.error("Handler error at cursor [{}]", cursor);
                        coordinator.error(t, eventTypePartition);
                    }
                }
            }
        }
    }

    private void handleEvents(final EventTypeCursor cursor, final List<T> events) {
        for (int i = 0; i < events.size(); i++) {
            final EventTypeCursor commitCursor = decreaseCursor(cursor, events.size() - (i + 1));
            try {
                delegate.onEvent(commitCursor, events.get(i));
                coordinator.commit(commitCursor);
            } catch (Throwable t) {
                log.error("Handler error at cursor [{}]", commitCursor);
                coordinator.error(t, eventTypePartition);
            }
        }
    }

    private EventTypeCursor decreaseCursor(final EventTypeCursor cursor, final int n) {
        if (n > 0) {
            try {
                final long nextOffset = Long.parseLong(cursor.getOffset()) - n;
                return EventTypeCursor.of(cursor.getEventTypePartition(), Long.toString(nextOffset));
            } catch (NumberFormatException ignore) {
                LOGGER.warn("Cannot advance cursor [{}] by [{}] due to [{}]", cursor, n, getMessage(ignore));

                // offset can be BEGIN
                return cursor;
            }
        } else {
            return cursor;
        }
    }

    abstract Optional<NakadiEventBatch<T>> getEventBatch(final String string);
}
