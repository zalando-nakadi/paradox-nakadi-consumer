package de.zalando.paradox.nakadi.consumer.boot;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.DefaultObjectMapper;
import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsResponseBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsResponseHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventResponseBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventResponseHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawContentHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawContentResponseHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventResponseBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventResponseHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.partitioned.impl.EmptyPartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

class ReplayHandler {

    private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper().jacksonObjectMapper();
    private static final String PARTITION_BEGIN = "BEGIN";

    private static final PartitionCoordinator THROWING_COORDINATOR = new EmptyPartitionCoordinator() {
        @Override
        public void error(final Throwable t, final EventTypePartition eventTypePartition) {
            ThrowableUtils.throwException(t);
        }
    };

    EventTypeCursor getQueryCursor(final EventTypeCursor cursor) {
        final String queryOffset;
        if (PARTITION_BEGIN.equals(cursor.getOffset())) {
            queryOffset = cursor.getOffset();
        } else {
            final long value = Long.parseLong(cursor.getOffset()) - 1;
            queryOffset = value >= 0 ? Long.toString(value) : PARTITION_BEGIN;
        }

        return EventTypeCursor.of(cursor.getEventTypePartition(), queryOffset);
    }

    @SuppressWarnings("unchecked")
    void handle(final EventHandler<?> handler, final EventTypePartition eventTypePartition, final String content) {
        if (handler instanceof RawContentHandler) {
            new RawContentResponseHandler(eventTypePartition, OBJECT_MAPPER, THROWING_COORDINATOR,
                (RawContentHandler) handler).onResponse(content);
        } else if (handler instanceof BatchEventsHandler) {
            new BatchEventsResponseHandler(eventTypePartition, OBJECT_MAPPER, THROWING_COORDINATOR,
                (BatchEventsHandler<?>) handler).onResponse(content);
        } else if (handler instanceof BatchEventsBulkHandler) {
            new BatchEventsResponseBulkHandler(eventTypePartition, OBJECT_MAPPER, THROWING_COORDINATOR,
                (BatchEventsBulkHandler<?>) handler).onResponse(content);
        } else if (handler instanceof RawEventHandler) {
            new RawEventResponseHandler(eventTypePartition, OBJECT_MAPPER, THROWING_COORDINATOR,
                (RawEventHandler) handler).onResponse(content);
        } else if (handler instanceof RawEventBulkHandler) {
            new RawEventResponseBulkHandler(eventTypePartition, OBJECT_MAPPER, THROWING_COORDINATOR,
                (RawEventBulkHandler) handler).onResponse(content);
        } else if (handler instanceof JsonEventHandler) {
            new JsonEventResponseHandler(eventTypePartition, OBJECT_MAPPER, THROWING_COORDINATOR,
                (JsonEventHandler) handler).onResponse(content);
        } else if (handler instanceof JsonEventBulkHandler) {
            new JsonEventResponseBulkHandler(eventTypePartition, OBJECT_MAPPER, THROWING_COORDINATOR,
                (JsonEventBulkHandler) handler).onResponse(content);
        } else {
            throw new IllegalStateException("Unknown handler type " + handler.getClass().getName());
        }
    }
}
