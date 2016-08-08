package de.zalando.paradox.nakadi.consumer.core.partitioned;

import java.util.Collection;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

public interface PartitionRebalanceListener {
    void onPartitionsAssigned(Collection<EventTypeCursor> cursors);

    void onPartitionsRevoked(Collection<EventTypePartition> partitions);

    void onPartitionsHealthCheck();
}
