package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Matchers.eq;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;

public class ZKLeaderConsumerPartitionRebalanceStrategyTest {

    private final AtomicInteger eventCounter = new AtomicInteger(0);
    private final ZKMember[] members;

    {
        members = new ZKMember[11];
        for (int i = 0; i < members.length; i++) {
            members[i] = ZKMember.of(Integer.toString(i));
        }
    }

    private ZKLeaderConsumerPartitionRebalanceStrategy strategy0;
    private ZKLeaderConsumerPartitionRebalanceStrategy strategy1;

    private ZKLeaderConsumerPartitionRebalanceStrategy.ResultCallback resultCallback;

    @Before
    public void setUp() {
        this.strategy0 = new ZKLeaderConsumerPartitionRebalanceStrategy(members[0]);
        this.strategy1 = new ZKLeaderConsumerPartitionRebalanceStrategy(members[1]);
        this.resultCallback = mock(ZKLeaderConsumerPartitionRebalanceStrategy.ResultCallback.class);
    }

    private EventType getEvent() {
        return EventType.of("EVENT" + eventCounter.incrementAndGet());
    }

    @Test
    public void multiEventHandling() {
        assignAllPartitionsToSingleMember();
        rebalanceMembersJoined0();
        orderOfPartitionsDoesNotMatter();
        rebalanceMembersJoined1();
        rebalanceMembersLeft0();
    }

    @Test
    public void doNothingWhenNoPartitions() {
        final EventType event1 = getEvent();
        strategy0.setNakadiPartitions(event1, nakadiPartitions());
        strategy0.setCurrentMembers(event1, newCurrentMember(members[0]));
        strategy0.rebalance(event1, resultCallback);
        verifyNoMoreInteractions(resultCallback);
    }

    @Test
    public void doNothingWhenNoCurrentMember() {
        final EventType event = getEvent();
        strategy0.setNakadiPartitions(event, nakadiPartitions(1));
        strategy0.rebalance(event, resultCallback);
        verifyNoMoreInteractions(resultCallback);
    }

    @Test
    public void assignAllPartitionsToSingleMember() {
        final EventType event = getEvent();

        for (int i = 1; i < 11; i++) {
            reset(resultCallback);

            final Collection<NakadiPartition> partitions = nakadiPartitions(i);
            strategy0.setNakadiPartitions(event, partitions);
            strategy0.setCurrentMembers(event, newCurrentMember(members[0]));
            strategy0.rebalance(event, resultCallback);

            final Verify verifyAssign = new Verify(event).invoke();
            assertThat(verifyAssign.getAssign()).isEqualTo(partitions);
            assertThat(verifyAssign.getRevoke()).hasSize(0);
        }
    }

    @Test
    public void rebalanceMembersJoined0() {
        final EventType event = getEvent();

        final List<NakadiPartition> partitions = nakadiPartitions(2);
        strategy0.setNakadiPartitions(event, partitions);
        strategy0.setCurrentMembers(event, newCurrentMember(members[0]));
        strategy0.rebalance(event, resultCallback);

        Verify verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo(partitions);
        assertThat(verifyAssign.getRevoke()).hasSize(0);

        // joined 2nd
        reset(resultCallback);
        strategy0.setCurrentMembers(event, newCurrentMember(members[0], members[1]));
        strategy0.rebalance(event, resultCallback);

        verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo(ImmutableList.of(partitions.get(0)));
        assertThat(verifyAssign.getRevoke()).isEqualTo(ImmutableList.of(partitions.get(1)));

        // joined 3d
        reset(resultCallback);
        strategy0.setCurrentMembers(event, newCurrentMember(members[0], members[1], members[2]));
        strategy0.rebalance(event, resultCallback);

        verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo((ImmutableList.of(partitions.get(0))));
        assertThat(verifyAssign.getRevoke()).isEqualTo((ImmutableList.of(partitions.get(1))));
    }

    @Test
    public void orderOfPartitionsDoesNotMatter() {
        final EventType event = getEvent();

        final List<NakadiPartition> partitions = nakadiPartitions(2);
        strategy1.setNakadiPartitions(event, ImmutableList.of(partitions.get(0), partitions.get(1)));
        strategy1.setCurrentMembers(event, newCurrentMember(members[0], members[1]));
        strategy1.rebalance(event, resultCallback);

        Verify verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo((ImmutableList.of(partitions.get(1))));
        assertThat(verifyAssign.getRevoke()).isEqualTo((ImmutableList.of(partitions.get(0))));

        reset(resultCallback);
        strategy1.setNakadiPartitions(event, ImmutableList.of(partitions.get(1), partitions.get(0)));
        strategy1.rebalance(event, resultCallback);

        verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo((ImmutableList.of(partitions.get(1))));
        assertThat(verifyAssign.getRevoke()).isEqualTo((ImmutableList.of(partitions.get(0))));
    }

    @Test
    public void rebalanceMembersJoined1() {
        final EventType event = getEvent();

        final List<NakadiPartition> partitions = nakadiPartitions(2);
        strategy1.setNakadiPartitions(event, partitions);
        strategy1.setCurrentMembers(event, newCurrentMember(members[0], members[1]));
        strategy1.rebalance(event, resultCallback);

        Verify verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo((ImmutableList.of(partitions.get(1))));
        assertThat(verifyAssign.getRevoke()).isEqualTo((ImmutableList.of(partitions.get(0))));

        // joined 3d
        reset(resultCallback);
        strategy1.setCurrentMembers(event, newCurrentMember(members[0], members[1], members[2]));
        strategy1.rebalance(event, resultCallback);

        assertThat(verifyAssign.getAssign()).isEqualTo((ImmutableList.of(partitions.get(1))));
        assertThat(verifyAssign.getRevoke()).isEqualTo((ImmutableList.of(partitions.get(0))));
    }

    @Test
    public void rebalanceMembersLeft0() {
        final EventType event = getEvent();

        final List<NakadiPartition> partitions = nakadiPartitions(2);
        strategy0.setNakadiPartitions(event, partitions);
        strategy0.setCurrentMembers(event, newCurrentMember(members[0], members[1], members[2]));
        strategy0.rebalance(event, resultCallback);

        Verify verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo((ImmutableList.of(partitions.get(0))));
        assertThat(verifyAssign.getRevoke()).isEqualTo((ImmutableList.of(partitions.get(1))));

        // left 2nd
        reset(resultCallback);
        strategy0.setCurrentMembers(event, newCurrentMember(members[0], members[2]));
        strategy0.rebalance(event, resultCallback);

        verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo((ImmutableList.of(partitions.get(0))));
        assertThat(verifyAssign.getRevoke()).isEqualTo((ImmutableList.of(partitions.get(1))));

        // left 3rd
        reset(resultCallback);
        strategy0.setCurrentMembers(event, newCurrentMember(members[0]));
        strategy0.rebalance(event, resultCallback);

        verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo((partitions));
        assertThat(verifyAssign.getRevoke()).hasSize(0);
    }

    @Test
    public void rebalanceMembersLeft1() {
        final EventType event = getEvent();

        final List<NakadiPartition> partitions = nakadiPartitions(2);
        strategy1.setNakadiPartitions(event, partitions);
        strategy1.setCurrentMembers(event, newCurrentMember(members[0], members[1], members[2]));
        strategy1.rebalance(event, resultCallback);

        Verify verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo(ImmutableList.of(partitions.get(1)));
        assertThat(verifyAssign.getRevoke()).isEqualTo(ImmutableList.of(partitions.get(0)));

        // left 1st
        reset(resultCallback);
        strategy1.setCurrentMembers(event, newCurrentMember(members[1], members[2]));
        strategy1.rebalance(event, resultCallback);
        verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo(ImmutableList.of(partitions.get(0)));
        assertThat(verifyAssign.getRevoke()).isEqualTo(ImmutableList.of(partitions.get(1)));

        // left 3rd
        reset(resultCallback);
        strategy1.setCurrentMembers(event, newCurrentMember(members[1]));
        strategy1.rebalance(event, resultCallback);
        verifyAssign = new Verify(event).invoke();
        assertThat(verifyAssign.getAssign()).isEqualTo(partitions);
        assertThat(verifyAssign.getRevoke()).hasSize(0);

    }

    private List<NakadiPartition> nakadiPartitions(final String... partitions) {
        final List<NakadiPartition> list = new ArrayList<>(partitions.length);
        for (String partition : partitions) {
            list.add(nakadiPartition(partition));
        }

        return list;
    }

    private List<NakadiPartition> nakadiPartitions(final int count) {
        final List<NakadiPartition> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(nakadiPartition(Integer.toString(i)));
        }

        return list;
    }

    private NakadiPartition nakadiPartition(final String partition) {
        return new NakadiPartition(partition, "0", "0");
    }

    private Map<String, ZKMember> newCurrentMember(final ZKMember... members) {
        return Arrays.stream(members).collect(Collectors.toMap(ZKMember::getMemberId, Function.identity()));
    }

    private class Verify {
        private EventType event1;
        private ArgumentCaptor<Collection<NakadiPartition>> assignCaptor;
        private ArgumentCaptor<Collection<NakadiPartition>> revokeCaptor;

        Verify(final EventType event1) {
            this.event1 = event1;
        }

        Verify invoke() {
            @SuppressWarnings("unchecked")
            final Class<Collection<NakadiPartition>> nakadiPartitionsClass = (Class<Collection<NakadiPartition>>)
                (Class) Collection.class;
            assignCaptor = ArgumentCaptor.forClass(nakadiPartitionsClass);
            revokeCaptor = ArgumentCaptor.forClass(nakadiPartitionsClass);
            verify(resultCallback, times(1)).rebalancePartitions(eq(event1), assignCaptor.capture(),
                revokeCaptor.capture());
            return this;
        }

        Collection<NakadiPartition> getAssign() {
            return assignCaptor.getValue();
        }

        Collection<NakadiPartition> getRevoke() {
            return revokeCaptor.getValue();
        }
    }
}
