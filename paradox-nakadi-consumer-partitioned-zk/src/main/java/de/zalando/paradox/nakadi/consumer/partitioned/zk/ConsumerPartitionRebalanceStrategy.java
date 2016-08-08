package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.Collection;
import java.util.Map;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;

public interface ConsumerPartitionRebalanceStrategy {

    interface ResultCallback {
        void rebalancePartitions(final EventType eventType, final Collection<NakadiPartition> partitionsToAssign,
                final Collection<NakadiPartition> partitionsToRevoke);
    }

    void rebalance(final EventType eventType, final ResultCallback resultCallback);

    void setNakadiPartitions(final EventType eventType, final Collection<NakadiPartition> collection);

    void setCurrentMembers(final EventType eventType, final Map<String, ZKMember> currentMember);
}
