package de.zalando.paradox.nakadi.consumer.core.partitioned;

import javax.annotation.Nullable;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;

@FunctionalInterface
public interface PartitionRebalanceListenerProvider {

    @Nullable
    PartitionRebalanceListener getPartitionRebalanceListener(final EventType eventType);

}
