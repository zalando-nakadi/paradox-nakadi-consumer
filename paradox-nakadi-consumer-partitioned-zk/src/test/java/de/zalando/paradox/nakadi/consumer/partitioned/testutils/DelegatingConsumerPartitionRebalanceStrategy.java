package de.zalando.paradox.nakadi.consumer.partitioned.testutils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import com.google.common.base.Preconditions;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ConsumerPartitionRebalanceStrategy;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ZKMember;

public class DelegatingConsumerPartitionRebalanceStrategy implements ConsumerPartitionRebalanceStrategy {

    private final ConsumerPartitionRebalanceStrategy[] delegates;

    public DelegatingConsumerPartitionRebalanceStrategy(final ConsumerPartitionRebalanceStrategy... delegates) {
        Preconditions.checkArgument(null != delegates && delegates.length > 0, "delegate must not be empty");
        this.delegates = delegates;
    }

    @Override
    public void rebalance(final EventType eventType, final ResultCallback resultCallback) {
        Arrays.stream(delegates).forEach(strategy -> strategy.rebalance(eventType, resultCallback));
    }

    @Override
    public void setNakadiPartitions(final EventType eventType, final Collection<NakadiPartition> collection) {
        Arrays.stream(delegates).forEach(strategy -> strategy.setNakadiPartitions(eventType, collection));
    }

    @Override
    public void setCurrentMembers(final EventType eventType, final Map<String, ZKMember> currentMember) {
        Arrays.stream(delegates).forEach(strategy -> strategy.setCurrentMembers(eventType, currentMember));
    }
}
