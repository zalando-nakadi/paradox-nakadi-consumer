package de.zalando.paradox.nakadi.consumer.core.partitioned.impl;

import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.LoggerFactory;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartitions;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallback;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class SimplePartitionCoordinator extends AbstractPartitionCoordinator {

    private final AtomicBoolean running = new AtomicBoolean(true);
    private boolean startNewestAvailableOffset = true;
    private final List<EventErrorHandler> eventErrorHandlerList;

    public SimplePartitionCoordinator() {
        this(Collections.emptyList());
    }

    public SimplePartitionCoordinator(final List<EventErrorHandler> eventErrorHandlers) {
        super(LoggerFactory.getLogger(SimplePartitionCoordinator.class));
        this.eventErrorHandlerList = eventErrorHandlers;
    }

    @Override
    public void init() {
        if (running.compareAndSet(false, true)) {
            log.info("Init coordinator");
        } else {
            log.info("Coordinator is already running");
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            log.info("Closing coordinator");
        } else {
            log.warn("Coordinator is already closed");
        }
    }

    @Override
    public void rebalance(final EventTypePartitions consumerPartitions,
            final Collection<NakadiPartition> nakadiPartitions) {
        if (running.get()) {
            revokePartitions(consumerPartitions.getEventType(),
                getPartitionsToRevoke(consumerPartitions, nakadiPartitions));
            assignPartitions(consumerPartitions.getEventType(),
                getPartitionsToAssign(consumerPartitions, nakadiPartitions), nakadiPartitions,
                getOffsetSelector(consumerPartitions.getEventType()));
        } else {
            log.warn("Coordinator is not running.");
        }
    }

    private Function<NakadiPartition, EventTypeCursor> getOffsetSelector(final EventType eventType) {
        return
            entry -> {
            final String offset;
            if (startNewestAvailableOffset) {
                offset = entry.getNewestAvailableOffset();
            } else {
                offset = entry.getOldestAvailableOffset();

                // messages will be replayed on each restart
                log.warn("Using oldest available offset [{}] without persistent storage.", offset);
            }

            return EventTypeCursor.of(EventTypePartition.of(eventType, entry.getPartition()), offset);
        };
    }

    @Override
    public void commit(final EventTypeCursor cursor) {
        log.debug("Commit {} ", cursor);

        final PartitionCommitCallback callback = getPartitionCommitCallback(cursor.getEventTypePartition());
        if (null != callback) {
            callback.onCommitComplete(cursor);
        }
    }

    @Override
    public void flush(final EventTypePartition eventTypePartition) {
        log.debug("Flush {} ", eventTypePartition);
    }

    @Override
    public void error(final Throwable t, final EventTypePartition eventTypePartition, final String cursor,
            final String rawEvent) {

        // it will unsubscribe reactive receiver
        if (ThrowableUtils.isUnrecoverableException(t)) {
            log.error("Error [{}] reason [{}]", eventTypePartition, getMessage(t));
            ThrowableUtils.throwException(t);
        } else {

            eventErrorHandlerList.forEach(eventErrorHandler ->
                    eventErrorHandler.onError(t, eventTypePartition, cursor, rawEvent));
            log.error("Error [{}] reason [{}]", eventTypePartition, getMessage(t), t);
        }
    }

    @Override
    public void error(final int statusCode, final String content, final EventTypePartition eventTypePartition) {
        log.error("Error [{}] code [{} / {}]", eventTypePartition, statusCode, content);
    }

    public void setStartNewestAvailableOffset(final boolean startNewestAvailableOffset) {
        this.startNewestAvailableOffset = startNewestAvailableOffset;
    }
}
