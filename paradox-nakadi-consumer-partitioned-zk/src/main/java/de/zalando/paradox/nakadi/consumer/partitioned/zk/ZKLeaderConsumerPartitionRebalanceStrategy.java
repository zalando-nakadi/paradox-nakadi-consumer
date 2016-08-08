package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;

public class ZKLeaderConsumerPartitionRebalanceStrategy implements ConsumerPartitionRebalanceStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKLeaderConsumerPartitionRebalanceStrategy.class);

    private final Map<EventType, Collection<NakadiPartition>> eventNakadiPartitions = new ConcurrentHashMap<>();
    private final Map<EventType, Map<String, ZKMember>> eventCurrentMembers = new ConcurrentHashMap<>();
    private final Map<EventType, Lock> eventLocks = new ConcurrentHashMap<>();
    private final ZKMember member;
    private final MembersOrdering membersOrdering;

    @FunctionalInterface
    public interface MembersOrdering {
        String[] getOrderedMembersIds(final Map<String, ZKMember> currentMembers);
    }

    public ZKLeaderConsumerPartitionRebalanceStrategy(final ZKMember member) {
        this(member, ZKMembersOrderings.MEMBERS_ID_ORDER);
    }

    public ZKLeaderConsumerPartitionRebalanceStrategy(final ZKMember member, final MembersOrdering membersOrdering) {
        this.member = requireNonNull(member, "member must not be null");
        this.membersOrdering = requireNonNull(membersOrdering, "membersOrdering must not be null");
    }

    @Override
    public void setNakadiPartitions(final EventType eventType, final Collection<NakadiPartition> collection) {
        eventNakadiPartitions.put(requireNonNull(eventType, "eventType must not be null"), collection);
    }

    @Override
    public void setCurrentMembers(final EventType eventType, final Map<String, ZKMember> currentMember) {
        eventCurrentMembers.put(requireNonNull(eventType, "eventType must not be null"), currentMember);
    }

    @Override
    public void rebalance(final EventType eventType, final ResultCallback resultCallback) {
        requireNonNull(eventType, "eventType must not be null");
        requireNonNull(resultCallback, "resultCallback must not be null");

        final Runnable runnable = () -> {

            final Collection<NakadiPartition> nakadiPartitions = eventNakadiPartitions.getOrDefault(eventType,
                    Collections.emptyList());
            final Map<String, ZKMember> currentMembers = eventCurrentMembers.getOrDefault(eventType,
                    Collections.emptyMap());

            LOGGER.debug("Rebalance input for [{}] member [{}] , all members [{}] , nakadi partitions [{}]", eventType,
                member.getMemberId(), currentMembers.keySet(), nakadiPartitions);

            if (!nakadiPartitions.isEmpty() && currentMembers.containsKey(member.getMemberId())) {

                if (currentMembers.size() == 1) {
                    resultCallback.rebalancePartitions(eventType, nakadiPartitions, Collections.emptyList());
                } else {
                    final Map<String, NakadiPartition> partitionsMap = nakadiPartitions.stream().collect(Collectors
                                .toMap(NakadiPartition::getPartition, Function.identity()));

                    final String[] partitionsArray = partitionsMap.keySet().toArray(new String[partitionsMap.size()]);
                    Arrays.sort(partitionsArray);

                    final String[] membersIdsArray = membersOrdering.getOrderedMembersIds(currentMembers);

                    final List<NakadiPartition> partitionsToAssign = new ArrayList<>();
                    final List<NakadiPartition> partitionsToRevoke = new ArrayList<>();

                    for (int i = 0; i < partitionsArray.length; i++) {
                        final int j = i % membersIdsArray.length;
                        final NakadiPartition nakadiPartition = partitionsMap.get(partitionsArray[i]);
                        Preconditions.checkState(null != nakadiPartition, "Partition [%s] value is missing",
                            partitionsArray[i]);

                        if (membersIdsArray[j].equals(member.getMemberId())) {
                            partitionsToAssign.add(nakadiPartition);
                        } else {
                            partitionsToRevoke.add(nakadiPartition);
                        }
                    }

                    resultCallback.rebalancePartitions(eventType, partitionsToAssign, partitionsToRevoke);
                }

            } else {
                LOGGER.info("More data is required to invoke rebalance for [{}] member", member.getMemberId());
            }
        };
        doWithLock(eventType, runnable);
    }

    private void doWithLock(final EventType eventType, final Runnable runnable) {
        final Lock lock = getLock(eventType);
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    private Lock getLock(final EventType eventType) {
        Lock lock = eventLocks.get(eventType);
        if (null == lock) {
            Lock oldLock = eventLocks.putIfAbsent(eventType, lock = new ReentrantLock());
            if (null != oldLock) {
                lock = oldLock;
            }
        }

        return lock;
    }
}
