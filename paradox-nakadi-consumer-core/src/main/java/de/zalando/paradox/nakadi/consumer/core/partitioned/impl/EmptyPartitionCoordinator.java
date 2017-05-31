package de.zalando.paradox.nakadi.consumer.core.partitioned.impl;

import java.util.Collection;

import javax.annotation.Nullable;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartitions;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallback;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListener;

public class EmptyPartitionCoordinator implements PartitionCoordinator {
    @Override
    public void rebalance(final EventTypePartitions consumerPartitions,
            final Collection<NakadiPartition> nakadiPartitions) { }

    @Override
    public void registerRebalanceListener(final EventType eventType, final PartitionRebalanceListener listener) { }

    @Override
    public void unregisterRebalanceListener(final EventType eventType) { }

    @Override
    public void registerCommitCallback(final EventTypePartition eventTypePartition,
            final PartitionCommitCallback callback) { }

    @Override
    public void unregisterCommitCallback(final EventTypePartition eventTypePartition) { }

    @Override
    public void commit(final EventTypeCursor cursor) { }

    @Override
    public void flush(final EventTypePartition eventTypePartition) { }

    @Override
    public void error(final String consumerName, final Throwable t, final EventTypePartition eventTypePartition,
            @Nullable final String cursor, final String rowEvent) { }

    @Override
    public void error(final int statusCode, final String content, final EventTypePartition eventTypePartition) { }
}
