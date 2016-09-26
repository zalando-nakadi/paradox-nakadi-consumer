package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.Collection;
import java.util.Map;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;

public interface ConsumerPartitionRebalanceStrategy {

    interface ResultCallback {
        void rebalancePartitions(EventType eventType, Collection<NakadiPartition> partitionsToAssign,
                final Collection<NakadiPartition> partitionsToRevoke);
    }

    void rebalance(EventType eventType, ResultCallback resultCallback);

    void setNakadiPartitions(EventType eventType, Collection<NakadiPartition> collection);

    void setCurrentMembers(EventType eventType, Map<String, ZKMember> currentMember);
}
