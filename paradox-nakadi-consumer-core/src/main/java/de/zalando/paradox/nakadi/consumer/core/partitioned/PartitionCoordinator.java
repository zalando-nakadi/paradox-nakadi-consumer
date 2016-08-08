package de.zalando.paradox.nakadi.consumer.core.partitioned;

import java.util.Collection;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartitions;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;

public interface PartitionCoordinator extends PartitionOffsetManagement {
    void rebalance(final EventTypePartitions consumerPartitions, final Collection<NakadiPartition> nakadiPartitions);

    void registerRebalanceListener(final EventType eventType, final PartitionRebalanceListener listener);

    void unregisterRebalanceListener(final EventType eventType);

    void registerCommitCallback(final EventTypePartition eventTypePartition, final PartitionCommitCallback callback);

    void unregisterCommitCallback(final EventTypePartition eventTypePartition);

    default void init() { }

    default void close() { }

    default void finished(final EventTypePartition eventTypePartition) {}
}
