package de.zalando.paradox.nakadi.consumer.core.partitioned;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

@FunctionalInterface
public interface PartitionCommitCallbackProvider {
    PartitionCommitCallback getPartitionCommitCallback(final EventTypePartition partition);
}
