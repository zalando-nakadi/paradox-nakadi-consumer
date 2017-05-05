package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static org.assertj.core.api.Assertions.assertThat;

import static com.google.common.collect.Iterables.getLast;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartitions;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListener;
import de.zalando.paradox.nakadi.consumer.partitioned.testutils.DelegatingConsumerPartitionRebalanceStrategy;
import de.zalando.paradox.nakadi.consumer.partitioned.testutils.MockConsumerPartitionRebalanceStrategy;

public class ZKLeaderConsumerPartitionCoordinatorTest extends AbstractZKTest {
    private static final EventType EVENT_TYPE = EventType.of("junit-event");

    @Mock
    private PartitionRebalanceListener partitionRebalanceListener;

    @Mock
    private ConsumerPartitionRebalanceStrategy rebalancerMock;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSingleMemberSetup() throws Exception {
        final String consumerName = "junit-consumer";
        final String memberId = "member-1";

        final String partition = "0";
        final NakadiPartition nakadiPartition = new NakadiPartition(partition, "BEGIN", "0", 0L);
        final Collection<NakadiPartition> nakadiPartitions = Collections.singleton(nakadiPartition);
        final EventTypePartitions consumerPartitons = new EventTypePartitions(EVENT_TYPE,
                Collections.singleton(partition));

        final MockConsumerPartitionRebalanceStrategy mockRebalancer = new MockConsumerPartitionRebalanceStrategy();
        final ZKLeaderConsumerPartitionCoordinator coordinator = getCoordinator(consumerName, mockRebalancer, memberId);
        coordinator.registerRebalanceListener(EVENT_TYPE, partitionRebalanceListener);

        // new member will be registered in the ZK
        mockRebalancer.init(2, 1, 1);
        coordinator.rebalance(consumerPartitons, nakadiPartitions);
        mockRebalancer.await(4, TimeUnit.SECONDS);
        mockRebalancer.verify();

        final Map<String, ZKMember> membersMap = getLast(mockRebalancer.verifyCurrentMembers());
        assertThat(membersMap).containsOnlyKeys(memberId);

        mockRebalancer.init(1, 1, 0);
        coordinator.rebalance(consumerPartitons, nakadiPartitions);
        mockRebalancer.await(4, TimeUnit.SECONDS);
        mockRebalancer.verify();
    }

    @Test
    public void test2MembersSetup() throws Exception {
        final String consumerName = "junit-consumer";
        final String memberId1 = "member-1";
        final String memberId2 = "member-2";

        final String partition = "0";
        final NakadiPartition nakadiPartition = new NakadiPartition(partition, "BEGIN", "0", 0L);
        final Collection<NakadiPartition> nakadiPartitions = Collections.singleton(nakadiPartition);
        final EventTypePartitions consumerPartitons = new EventTypePartitions(EVENT_TYPE,
                Collections.singleton(partition));

        final MockConsumerPartitionRebalanceStrategy mockRebalancer1 = new MockConsumerPartitionRebalanceStrategy();
        final ZKLeaderConsumerPartitionCoordinator coordinator1 = getCoordinator(consumerName, mockRebalancer1,
                memberId1);
        coordinator1.registerRebalanceListener(EVENT_TYPE, partitionRebalanceListener);

        // new member will be registered in the ZK
        mockRebalancer1.init(2, 1, 1);
        coordinator1.rebalance(consumerPartitons, nakadiPartitions);
        mockRebalancer1.await(4, TimeUnit.SECONDS);
        mockRebalancer1.verify();

        final MockConsumerPartitionRebalanceStrategy mockRebalancer2 = new MockConsumerPartitionRebalanceStrategy();
        final ZKLeaderConsumerPartitionCoordinator coordinator2 = getCoordinator(consumerName, mockRebalancer2,
                memberId2);
        coordinator2.registerRebalanceListener(EVENT_TYPE, partitionRebalanceListener);

        mockRebalancer1.init(1, 0, 1); // member 1 will be notified about 2
        mockRebalancer2.init(3, 1, 2); // member 1 will be notified about 1

        coordinator2.rebalance(consumerPartitons, nakadiPartitions);
        mockRebalancer2.await(4, TimeUnit.SECONDS);
        mockRebalancer2.verify();
        assertThat(getLast(mockRebalancer2.verifyCurrentMembers())).containsOnlyKeys(memberId1, memberId2);

        mockRebalancer1.await(4, TimeUnit.SECONDS);
        mockRebalancer1.verify();
        assertThat(getLast(mockRebalancer1.verifyCurrentMembers())).containsOnlyKeys(memberId1, memberId2);

        mockRebalancer1.init(1, 0, 1);
        coordinator2.close();
        mockRebalancer1.await(4, TimeUnit.SECONDS);
        mockRebalancer1.verify();
        assertThat(getLast(mockRebalancer1.verifyCurrentMembers())).containsOnlyKeys(memberId1);
    }

    private ZKLeaderConsumerPartitionCoordinator getCoordinator(final String consumerName,
            final MockConsumerPartitionRebalanceStrategy mockRebalancer, final String memberId) {
        ZKMember member = ZKMember.of(memberId);
        ZKConsumerGroupMember consumerGroupMember = new ZKConsumerGroupMember(zkHolder, consumerName, member);
        ZKLeaderConsumerPartitionRebalanceStrategy rebalancer = new ZKLeaderConsumerPartitionRebalanceStrategy(member);
        ZKConsumerPartitionLeader consumerPartitionLeader = new ZKConsumerPartitionLeader(zkHolder, consumerName,
                member);

        return new ZKLeaderConsumerPartitionCoordinator(zkHolder, consumerName, member, consumerGroupMember,
                new DelegatingConsumerPartitionRebalanceStrategy(rebalancer, mockRebalancer, rebalancerMock),
                consumerPartitionLeader, Collections.emptyList());
    }
}
