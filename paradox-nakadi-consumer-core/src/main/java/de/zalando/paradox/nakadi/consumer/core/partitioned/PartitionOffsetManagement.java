package de.zalando.paradox.nakadi.consumer.core.partitioned;

import javax.annotation.Nullable;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

public interface PartitionOffsetManagement {
    void commit(final EventTypeCursor cursor);

    void flush(final EventTypePartition eventTypePartition);

    void error(Throwable t, EventTypePartition eventTypePartition, @Nullable String offset, String rawEvent);

    void error(final int statusCode, final String content, final EventTypePartition eventTypePartition);
}
