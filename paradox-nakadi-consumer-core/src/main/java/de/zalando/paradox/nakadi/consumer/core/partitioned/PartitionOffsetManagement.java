package de.zalando.paradox.nakadi.consumer.core.partitioned;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

public interface PartitionOffsetManagement {
    void commit(final EventTypeCursor cursor);

    void flush(final EventTypePartition eventTypePartition);

    void error(final Throwable t, final EventTypePartition eventTypePartition);

    void error(final int statusCode, final String content, final EventTypePartition eventTypePartition);
}
