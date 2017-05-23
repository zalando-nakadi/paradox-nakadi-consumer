package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.util.Objects.requireNonNull;

import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallback;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallbackProvider;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionOffsetManagement;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListener;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListenerProvider;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

class ZKConsumerSyncOffsetManagement implements PartitionOffsetManagement {

    private static final Pattern CURSOR_NOT_AVAILABLE = Pattern.compile(
            "offset \\S+ for partition \\S+ is unavailable");

    private static final int PRECONDITION_FAILED_HTTP_CODE = 412;

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKConsumerSyncOffsetManagement.class);

    private volatile boolean deleteUnavailableCursors;

    private final PartitionCommitCallbackProvider commitCallbackProvider;

    private final PartitionRebalanceListenerProvider rebalanceListenerProvider;

    private final ZKConsumerOffset consumerOffset;

    private final List<EventErrorHandler> eventErrorHandlers;

    ZKConsumerSyncOffsetManagement(@Nonnull final ZKConsumerOffset consumerOffset,
            @Nonnull final PartitionCommitCallbackProvider commitCallbackProvider,
            @Nonnull final PartitionRebalanceListenerProvider rebalanceListenerProvider,
            @Nonnull final List<EventErrorHandler> eventErrorHandlers) {
        this.commitCallbackProvider = commitCallbackProvider;
        this.rebalanceListenerProvider = rebalanceListenerProvider;
        this.consumerOffset = consumerOffset;
        this.eventErrorHandlers = requireNonNull(eventErrorHandlers);
    }

    @Override
    public void commit(final EventTypeCursor cursor) {
        LOGGER.debug("Commit [{}] ", cursor);

        try {
            consumerOffset.setOffset(cursor);
        } catch (Exception e) {
            ThrowableUtils.throwException(e);
        }

        final PartitionCommitCallback callback = commitCallbackProvider.getPartitionCommitCallback(
                cursor.getEventTypePartition());
        if (null != callback) {
            callback.onCommitComplete(cursor);
        }
    }

    @Override
    public void flush(final EventTypePartition eventTypePartition) {
        LOGGER.debug("Flush [{}] ", eventTypePartition);
    }

    @Override
    public void error(final String consumerName, final Throwable t, final EventTypePartition eventTypePartition,
            @Nullable final String offset, final String rawEvent) {

        // it will unsubscribe reactive receiver
        if (ThrowableUtils.isUnrecoverableException(t)) {
            LOGGER.error("Error [{}] reason [{}]", eventTypePartition, getMessage(t));
            ThrowableUtils.throwException(t);
        } else {
            eventErrorHandlers.forEach(eventErrorHandler ->
                    eventErrorHandler.onError(consumerName, t, eventTypePartition, offset, rawEvent));

            LOGGER.error("Error [{}] reason [{}] raw event [{}] ", eventTypePartition, getMessage(t), rawEvent, t);
        }
    }

    @Override
    public void error(final int statusCode, final String content, final EventTypePartition eventTypePartition) {
        LOGGER.error("Consumer [{}] error [{}] / [{}] for [{}] ", consumerOffset.getConsumerName(), statusCode, content,
            eventTypePartition);

        // error [412] / [{"type":"http://httpstatus.es/412","title":"Precondition
        // Failed","status":412,"detail":"offset 34505189 for partition 0 is unavailable"}]
        if (deleteUnavailableCursors && statusCode == PRECONDITION_FAILED_HTTP_CODE
                && CURSOR_NOT_AVAILABLE.matcher(content).find()) {

            final String path = consumerOffset.getOffsetPath(eventTypePartition.getName(),
                    eventTypePartition.getPartition());
            try {
                LOGGER.warn("Delete consumer offset [{}] due to error [{}]", path, content);
                consumerOffset.delOffset(path);

                // partition will be restarted later and it will use new offset
                final PartitionRebalanceListener listener = rebalanceListenerProvider.getPartitionRebalanceListener(
                        eventTypePartition.getEventType());
                if (null != listener) {
                    LOGGER.warn("Trying to stop consumer [{}] partition [{}]", consumerOffset.getConsumerName(),
                        eventTypePartition);
                    listener.onPartitionsRevoked(Collections.singleton(eventTypePartition));
                }
            } catch (Exception e) {
                ThrowableUtils.throwException(e);
            }

        }
    }

    void setDeleteUnavailableCursors(final boolean deleteUnavailableCursors) {
        this.deleteUnavailableCursors = deleteUnavailableCursors;
    }

}
