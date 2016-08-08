package de.zalando.paradox.nakadi.consumer.core.partitioned;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;

@FunctionalInterface
public interface PartitionCommitCallback {
    void onCommitComplete(final EventTypeCursor cursor);
}
