package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallback;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallbackProvider;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionOffsetManagement;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListener;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListenerProvider;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

class ZKConsumerSyncOffsetManagement implements PartitionOffsetManagement {
    private static final String CURSORS_ARE_NOT_VALID = "cursors are not valid";

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKConsumerSyncOffsetManagement.class);

    private boolean resetStorageOnInvalidCursor = false;

    private final PartitionCommitCallbackProvider commitCallbackProvider;
    private final PartitionRebalanceListenerProvider rebalanceListenerProvider;
    private final ZKConsumerOffset consumerOffset;

    ZKConsumerSyncOffsetManagement(final ZKConsumerOffset consumerOffset,
            final PartitionCommitCallbackProvider commitCallbackProvider,
            final PartitionRebalanceListenerProvider rebalanceListenerProvider) {
        this.commitCallbackProvider = commitCallbackProvider;
        this.rebalanceListenerProvider = rebalanceListenerProvider;
        this.consumerOffset = consumerOffset;
    }

    @Override
    public void commit(final EventTypeCursor cursor) {
        LOGGER.debug("Commit {} ", cursor);

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
        LOGGER.debug("flush {} ", eventTypePartition);
    }

    @Override
    public void error(final Throwable t, final EventTypePartition eventTypePartition) {

        // it will unsubscribe reactive receiver
        if (ThrowableUtils.isUnrecoverableException(t)) {
            LOGGER.error("Error [{}] reason [{}]", eventTypePartition, getMessage(t));
            ThrowableUtils.throwException(t);
        } else {
            LOGGER.error("Error [{}] reason [{}]", eventTypePartition, getMessage(t), t);
        }
    }

    @Override
    public void error(final int statusCode, final String content, final EventTypePartition eventTypePartition) {
        LOGGER.error("Consumer [{}] error [{}] / [{}] for [{}] ", consumerOffset.getConsumerName(), statusCode, content,
            eventTypePartition);

        // error [412] / [{"type":"http://httpstatus.es/412","title":"Precondition
        // Failed","status":412,"detail":"cursors are not valid"}]
        if (resetStorageOnInvalidCursor && statusCode == 412 && content.contains(CURSORS_ARE_NOT_VALID)) {
            final String path = consumerOffset.getOffsetPath(eventTypePartition.getName(),
                    eventTypePartition.getPartition());
            try {
                LOGGER.warn("Delete consumer offset [{}] due to error [{}]", path, content);
                consumerOffset.delOffset(path);

                // partition will be restarted later and it with use new offset
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

    public void setResetStorageOnInvalidCursor(final boolean resetStorageOnInvalidCursor) {
        this.resetStorageOnInvalidCursor = resetStorageOnInvalidCursor;
    }
}
