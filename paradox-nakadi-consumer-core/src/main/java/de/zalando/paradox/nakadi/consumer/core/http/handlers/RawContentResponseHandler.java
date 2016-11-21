package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Preconditions;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventCursor;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.LoggingUtils;

public class RawContentResponseHandler extends AbstractResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RawContentResponseHandler.class);

    private final RawContentHandler delegate;

    public RawContentResponseHandler(final EventTypePartition eventTypePartition, final ObjectMapper jsonMapper,
            final PartitionCoordinator coordinator, final RawContentHandler delegate) {
        super(eventTypePartition, coordinator,
            LoggingUtils.getLogger(RawContentResponseHandler.class, eventTypePartition), jsonMapper);
        this.delegate = delegate;
    }

    @Override
    public void onResponse(final String content) {
        final String[] events = getEvents(content);
        if (events.length > 0) {
            final NakadiEventCursor nakadiFirstCursor = getEventCursor(events[0]);

            if (null != nakadiFirstCursor) {

                final NakadiEventCursor nakadiLastCursor = events.length > 1 ? getEventCursor(events[events.length - 1])
                                                                             : nakadiFirstCursor;

                final EventTypeCursor firstCursor = EventTypeCursor.of(eventTypePartition,
                        nakadiFirstCursor.getCursor().getOffset());
                final EventTypeCursor lastCursor = EventTypeCursor.of(eventTypePartition,
                        nakadiLastCursor.getCursor().getOffset());

                Preconditions.checkArgument(firstCursor.getPartition().equals(lastCursor.getPartition()),
                    "Cursor partitions differ [%s] vs [%s]", firstCursor.getPartition(), lastCursor.getPartition());

                try {
                    delegate.onEvent(lastCursor, content);
                } catch (Throwable t) {
                    LOGGER.error("Handler error at firstCursor [{}] , lastCursor [{}]", firstCursor, lastCursor);
                    coordinator.error(t, eventTypePartition, lastCursor.getOffset(), content);
                }

                coordinator.commit(lastCursor);
            }
        }
    }
}
