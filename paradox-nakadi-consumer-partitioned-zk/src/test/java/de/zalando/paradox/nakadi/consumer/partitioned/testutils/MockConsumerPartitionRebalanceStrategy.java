package de.zalando.paradox.nakadi.consumer.partitioned.testutils;

import static org.mockito.Mockito.times;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ConsumerPartitionRebalanceStrategy;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ZKMember;

public class MockConsumerPartitionRebalanceStrategy implements ConsumerPartitionRebalanceStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockConsumerPartitionRebalanceStrategy.class);

    private CountDownLatch rebalanceLatch = new CountDownLatch(0);
    private CountDownLatch nakadiPartitionsLatch = new CountDownLatch(0);
    private CountDownLatch currentMembersLatch = new CountDownLatch(0);

    private int rebalanceInvocations;
    private int nakadiPartitionsInvocations;
    private int currentMembersInvocations;

    private ConsumerPartitionRebalanceStrategy mockDelegate = Mockito.mock(ConsumerPartitionRebalanceStrategy.class);

    public void init(final int rebalanceInvocations, final int nakadiPartitionsInvocations,
            final int currentMembersInvocations) {

        this.rebalanceLatch = new CountDownLatch(rebalanceInvocations);
        this.nakadiPartitionsLatch = new CountDownLatch(nakadiPartitionsInvocations);
        this.currentMembersLatch = new CountDownLatch(currentMembersInvocations);

        this.rebalanceInvocations = rebalanceInvocations;
        this.nakadiPartitionsInvocations = nakadiPartitionsInvocations;
        this.currentMembersInvocations = currentMembersInvocations;

        this.mockDelegate = Mockito.mock(ConsumerPartitionRebalanceStrategy.class);
    }

    @Override
    public void rebalance(final EventType eventType, final ResultCallback resultCallback) {
        LOGGER.info("rebalance");
        mockDelegate.rebalance(eventType, resultCallback);
        rebalanceLatch.countDown();
    }

    @Override
    public void setNakadiPartitions(final EventType eventType, final Collection<NakadiPartition> collection) {
        LOGGER.info("setNakadiPartitions");
        mockDelegate.setNakadiPartitions(eventType, collection);
        nakadiPartitionsLatch.countDown();
    }

    @Override
    public void setCurrentMembers(final EventType eventType, final Map<String, ZKMember> currentMember) {
        LOGGER.info("setCurrentMembers");
        mockDelegate.setCurrentMembers(eventType, currentMember);
        currentMembersLatch.countDown();
    }

    public void await() throws InterruptedException {
        rebalanceLatch.await();
        nakadiPartitionsLatch.await();
        currentMembersLatch.await();
    }

    public void await(final long timeout, final TimeUnit unit) throws InterruptedException {
        rebalanceLatch.await(timeout, unit);
        nakadiPartitionsLatch.await(timeout, unit);
        currentMembersLatch.await(timeout, unit);
    }

    public void verify() {
        verifyRebalance();
        verifyCurrentMembers();
        verifyNakadiPartitions();
    }

    public List<Collection<NakadiPartition>> verifyNakadiPartitions() {
        final ArgumentCaptor<EventType> eventTypeCaptor = ArgumentCaptor.forClass(EventType.class);
        final ArgumentCaptor<Collection> nakadiPartitionsCaptor = ArgumentCaptor.forClass(Collection.class);

        Mockito.verify(mockDelegate, times(nakadiPartitionsInvocations)).setNakadiPartitions(eventTypeCaptor.capture(),
            nakadiPartitionsCaptor.capture());
        return (List<Collection<NakadiPartition>>) (List<?>) nakadiPartitionsCaptor.getAllValues();
    }

    public List<Map<String, ZKMember>> verifyCurrentMembers() {
        final ArgumentCaptor<EventType> eventTypeCaptor = ArgumentCaptor.forClass(EventType.class);
        final ArgumentCaptor<Map> currentMembersCaptor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(mockDelegate, times(currentMembersInvocations)).setCurrentMembers(eventTypeCaptor.capture(),
            currentMembersCaptor.capture());

        return (List<Map<String, ZKMember>>) (List<?>) currentMembersCaptor.getAllValues();
    }

    public List<ResultCallback> verifyRebalance() {
        final ArgumentCaptor<EventType> eventTypeCaptor = ArgumentCaptor.forClass(EventType.class);
        final ArgumentCaptor<ResultCallback> resultCallbackCaptor = ArgumentCaptor.forClass(ResultCallback.class);

        Mockito.verify(mockDelegate, times(rebalanceInvocations)).rebalance(eventTypeCaptor.capture(),
            resultCallbackCaptor.capture());

        return resultCallbackCaptor.getAllValues();
    }
}
