package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.json.JSONException;

import com.google.common.annotations.VisibleForTesting;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartitions;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.utils.LoggingUtils;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class ZKLeaderConsumerPartitionCoordinator extends AbstractZKConsumerPartitionCoordinator {
    private final ZKMember member;
    private final ConcurrentMap<EventType, ZKGroupMember> groupMembers = new ConcurrentHashMap<>();
    private final ZKConsumerGroupMember consumerGroupMember;
    private final ConsumerPartitionRebalanceStrategy rebalancer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final ZKConsumerPartitionLeader consumerPartitionLeader;
    private final ConsumerPartitionRebalanceStrategy.ResultCallback rebalanceResultCallback =
        getResultCallbackHandler();

    private ConsumerPartitionRebalanceStrategy.ResultCallback getResultCallbackHandler() {
        return
            (eventType, nakadiPartitionsToAssign, nakadiPartitionsToRevoke) -> {
            if (running.get()) {

                log.debug("Rebalance [{}], assign [{}], revoke [{}]", eventType.getName(),
                    getPartitions(nakadiPartitionsToAssign), getPartitions(nakadiPartitionsToRevoke));

                nakadiPartitionsToRevoke.forEach(nakadiPartitionToRevoke -> {
                    try {
                        consumerPartitionLeader.closeGroupLeadership(
                            EventTypePartition.of(eventType, nakadiPartitionToRevoke.getPartition()));
                    } finally {
                        revokePartition(eventType, nakadiPartitionToRevoke.getPartition());
                    }
                });

                nakadiPartitionsToAssign.forEach(nakadiPartitionToAssign -> {
                    try {
                        consumerPartitionLeader.initGroupLeadership(
                            EventTypePartition.of(eventType, nakadiPartitionToAssign.getPartition()),
                            new ZKConsumerLeader.LeadershipChangedListener<EventTypePartition>() {
                                @Override
                                public void takeLeadership(final EventTypePartition eventTypePartition,
                                        final ZKMember member) {
                                    log.info("Member [{}] takes leadership [{}]", member.getMemberId(),
                                        eventTypePartition);

                                    final Function<NakadiPartition, EventTypeCursor> offsetSelector = getOffsetSelector(
                                            eventTypePartition.getEventType());
                                    assignPartition(eventTypePartition.getEventType(), nakadiPartitionToAssign,
                                        offsetSelector);
                                }

                                @Override
                                public void relinquishLeadership(final EventTypePartition eventTypePartition,
                                        final ZKMember member) {
                                    log.info("Member [{}] relinquishes leadership [{}]", member.getMemberId(),
                                        eventTypePartition);
                                    revokePartition(eventTypePartition.getEventType(),
                                        eventTypePartition.getPartition());
                                }
                            });

                    } catch (Exception e) {
                        ThrowableUtils.throwException(e);
                    }
                });
            }
        };
    }

    public ZKLeaderConsumerPartitionCoordinator(final ZKHolder zkHolder, final String consumerName,
            final List<EventErrorHandler> eventErrorHandlers) {
        super(LoggingUtils.getLogger(ZKLeaderConsumerPartitionCoordinator.class, consumerName), zkHolder, consumerName,
            eventErrorHandlers);
        this.member = ZKMember.of(UUID.randomUUID().toString());
        this.consumerGroupMember = new ZKConsumerGroupMember(zkHolder, consumerName, member);
        this.rebalancer = new ZKLeaderConsumerPartitionRebalanceStrategy(member);
        this.consumerPartitionLeader = new ZKConsumerPartitionLeader(zkHolder, consumerName, member);
    }

    @VisibleForTesting
    ZKLeaderConsumerPartitionCoordinator(final ZKHolder zkHolder, final String consumerName, final ZKMember member,
            final ZKConsumerGroupMember consumerGroupMember, final ConsumerPartitionRebalanceStrategy rebalancer,
            final ZKConsumerPartitionLeader consumerPartitionLeader, final List<EventErrorHandler> eventErrorHandlers) {
        super(LoggingUtils.getLogger(ZKLeaderConsumerPartitionCoordinator.class, consumerName), zkHolder, consumerName,
            eventErrorHandlers);
        this.member = requireNonNull(member);
        this.consumerGroupMember = requireNonNull(consumerGroupMember);
        this.rebalancer = requireNonNull(rebalancer);
        this.consumerPartitionLeader = requireNonNull(consumerPartitionLeader);
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            log.info("Closing coordinator for member [{}]", member.getMemberId());
            groupMembers.entrySet().forEach(groupMember -> groupMember.getValue().close());
            groupMembers.clear();
            consumerPartitionLeader.close();
        } else {
            log.warn("Coordinator for member [{}] is already closed", member.getMemberId());
        }
    }

    @Override
    public void init() {
        if (running.compareAndSet(false, true)) {
            log.info("Init coordinator for member [{}]", member.getMemberId());
        } else {
            log.info("Coordinator for member [{}] is already running", member.getMemberId());
        }
    }

    @Override
    public void rebalance(final EventTypePartitions consumerPartitions,
            final Collection<NakadiPartition> nakadiPartitions) {
        if (running.get()) {
            rebalance0(consumerPartitions, nakadiPartitions);
        } else {
            log.warn("ConsumerPartitionCoordinator is not running.");
        }
    }

    private void rebalance0(final EventTypePartitions consumerPartitions,
            final Collection<NakadiPartition> nakadiPartitions) {

        revokePartitions(consumerPartitions.getEventType(),
            getPartitionsToRevoke(consumerPartitions, nakadiPartitions));

        rebalancer.setNakadiPartitions(consumerPartitions.getEventType(), nakadiPartitions);
        rebalancer.rebalance(consumerPartitions.getEventType(), rebalanceResultCallback);

        joinGroup(consumerPartitions.getEventType());
    }

    @Override
    public void finished(final EventTypePartition eventTypePartition) {
        log.info("Close group leadership on finished [{}]", eventTypePartition);
        try {
            consumerPartitionLeader.closeGroupLeadership(eventTypePartition);
        } finally {
            revokePartition(eventTypePartition.getEventType(), eventTypePartition.getPartition());
        }
    }

    private void joinGroup(final EventType eventType) {
        ZKGroupMember groupMember = groupMembers.get(eventType);
        if (null == groupMember) {
            groupMember = consumerGroupMember.newGroupMember(eventType, newGroupChangedListener());
            if (null == groupMembers.putIfAbsent(eventType, groupMember)) {
                try {
                    log.info("Member [{}] is joining group for event type [{}] ", member.getMemberId(), eventType);
                    groupMember.start();
                } catch (Throwable t) {
                    leaveGroup(eventType);
                    ThrowableUtils.throwException(t);
                }
            }
        }
    }

    private ZKConsumerGroupMember.GroupChangedListener newGroupChangedListener() {
        return new ZKConsumerGroupMember.GroupChangedListener() {
            @Override
            public void memberAdded(final EventType eventType, final String memberId) {
                final String own = member.getMemberId().equals(memberId) ? "This" : "Other";
                log.info("[{}] member [{}] joined group for event type [{}]", own, memberId, eventType, memberId);
                onGroupChanged(eventType);
            }

            @Override
            public void memberRemoved(final EventType eventType, final String memberId) {
                final String own = member.getMemberId().equals(memberId) ? "This" : "Other";
                log.info("[{}] member [{}] left group for event type [{}]", own, memberId, eventType, memberId);
                onGroupChanged(eventType);
            }

            private Map<String, ZKMember> getCurrentMembers(final EventType eventType) {
                final ZKGroupMember groupMember = groupMembers.get(eventType);
                if (null != groupMember) {
                    final Function<Map.Entry<String, byte[]>, ZKMember> valueMapper = entry -> {
                        try {
                            return ZKMember.fromByteJson(entry.getValue());
                        } catch (JSONException e) {
                            log.error("Cannot decode ZK data of member [{}]", entry.getKey(), e);
                            return null;
                        }
                    };
                    return groupMember.getCurrentMembers().entrySet().stream().collect(Collectors.toMap(
                                Map.Entry::getKey, valueMapper));
                } else {
                    return Collections.emptyMap();
                }
            }

            private void onGroupChanged(final EventType eventType) {
                rebalancer.setCurrentMembers(eventType, getCurrentMembers(eventType));
                rebalancer.rebalance(eventType, rebalanceResultCallback);
            }
        };
    }

    private void leaveGroup(final EventType eventType) {
        final ZKGroupMember groupMember = groupMembers.remove(eventType);
        if (null != groupMember) {
            log.info("Member [{}] is leaving group for event type [{}]", member.getMemberId(), eventType);
            groupMember.close();
        }
    }
}
