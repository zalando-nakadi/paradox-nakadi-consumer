package de.zalando.paradox.nakadi.consumer.boot.components;

import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public interface ConsumerPartitionCoordinatorProvider {
    PartitionCoordinator getPartitionCoordinator(final String consumerName);
}
