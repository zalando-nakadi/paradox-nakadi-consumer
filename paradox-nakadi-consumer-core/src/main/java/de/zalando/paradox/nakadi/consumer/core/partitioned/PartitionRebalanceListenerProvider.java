package de.zalando.paradox.nakadi.consumer.core.partitioned;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;

@FunctionalInterface
public interface PartitionRebalanceListenerProvider {
    PartitionRebalanceListener getPartitionRebalanceListener(final EventType eventType);
}
