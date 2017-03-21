package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartitions;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.utils.LoggingUtils;

public class ZKSimpleConsumerPartitionCoordinator extends AbstractZKConsumerPartitionCoordinator {

    private final AtomicBoolean running = new AtomicBoolean(true);

    public ZKSimpleConsumerPartitionCoordinator(final ZKHolder zkHolder, final String consumerName,
            final List<EventErrorHandler> eventErrorHandlerList) {
        super(LoggingUtils.getLogger(ZKLeaderConsumerPartitionCoordinator.class, consumerName), zkHolder, consumerName,
            eventErrorHandlerList);
    }

    @Override
    public void rebalance(final EventTypePartitions consumerPartitions,
            final Collection<NakadiPartition> nakadiPartitions) {
        if (running.get()) {
            revokePartitions(consumerPartitions.getEventType(),
                getPartitionsToRevoke(consumerPartitions, nakadiPartitions));
            assignPartitions(consumerPartitions.getEventType(),
                getPartitionsToAssign(consumerPartitions, nakadiPartitions), nakadiPartitions,
                getOffsetSelector(consumerPartitions.getEventType()));
        } else {
            log.warn("Coordinator is not running.");
        }
    }

    @Override
    public void init() {
        if (running.compareAndSet(false, true)) {
            log.info("Init coordinator");
        } else {
            log.info("Coordinator is already running");
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            log.info("Closing coordinator");
        } else {
            log.warn("Coordinator is already closed");
        }
    }
}
