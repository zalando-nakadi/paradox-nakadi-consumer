package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.lang.String.format;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

public class ZKConsumerPartitionLeader extends ZKConsumerLeader<EventTypePartition> {
    private static final String PARTITION_LEADER_SELECTOR =
        "/paradox/nakadi/event_types/%s/partitions/%s/consumers/%s/leader_selector";
    private static final String PARTITION_LEADER_INFO =
        "/paradox/nakadi/event_types/%s/partitions/%s/consumers/%s/leader_info";

    ZKConsumerPartitionLeader(final ZKHolder zkHolder, final String consumerName, final ZKMember member) {
        super(zkHolder, consumerName, member);
    }

    @Override
    public String getLeaderSelectorPath(final EventTypePartition eventTypePartition) {
        return format(PARTITION_LEADER_SELECTOR, eventTypePartition.getName(), eventTypePartition.getPartition(),
                getConsumerName());
    }

    @Override
    public String getLeaderInfoPath(final EventTypePartition eventTypePartition) {
        return format(PARTITION_LEADER_INFO, eventTypePartition.getName(), eventTypePartition.getPartition(),
                getConsumerName());
    }
}
