package de.zalando.paradox.nakadi.consumer.core.partitioned.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartitions;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallback;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallbackProvider;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListener;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListenerProvider;

public abstract class AbstractPartitionCoordinator implements PartitionCoordinator, PartitionCommitCallbackProvider,
    PartitionRebalanceListenerProvider {

    protected final Logger log;

    private final ConcurrentMap<EventType, PartitionRebalanceListener> rebalanceListeners = new ConcurrentHashMap<>();

    private final ConcurrentMap<EventTypePartition, PartitionCommitCallback> commitCallbacks =
        new ConcurrentHashMap<>();

    protected AbstractPartitionCoordinator(final Logger log) {
        this.log = log;
    }

    @Override
    public void registerRebalanceListener(final EventType eventType, final PartitionRebalanceListener listener) {
        log.info("Register PartitionRebalanceListener for [{}]", eventType);
        Preconditions.checkState(null == rebalanceListeners.putIfAbsent(eventType, listener),
            "PartitionRebalanceListener for [%s] already registered", eventType);
    }

    @Override
    public void unregisterRebalanceListener(final EventType eventType) {
        log.info("Unregister PartitionRebalanceListener for [{}]", eventType);
        if (null == rebalanceListeners.remove(eventType)) {
            log.warn("PartitionRebalanceListener for [{}] is already unregistered", eventType);
        }
    }

    @Override
    public void registerCommitCallback(final EventTypePartition eventTypePartition,
            final PartitionCommitCallback callback) {

        final PartitionCommitCallback registeredPartitionCommitCallback = commitCallbacks.putIfAbsent(
                eventTypePartition, callback);

        Preconditions.checkState(null == registeredPartitionCommitCallback,
            "PartitionCommitCallback for [%s] already registered (try to register callback [%s] but found [%s])",
            eventTypePartition, callback, registeredPartitionCommitCallback);
    }

    @Override
    public void unregisterCommitCallback(final EventTypePartition eventTypePartition) {
        if (null == commitCallbacks.remove(eventTypePartition)) {
            log.warn("PartitionCommitCallback for [%s] is already unregistered", eventTypePartition);
        }
    }

    @Override
    public void finished(final EventTypePartition eventTypePartition) {
        log.info("Revoke partition on finished [{}]", eventTypePartition);
        revokePartition(eventTypePartition.getEventType(), eventTypePartition.getPartition());
    }

    protected Set<String> getPartitionsToAssign(final EventTypePartitions consumerPartitions,
            final Collection<NakadiPartition> nakadiPartitions) {
        final Set<String> currentPartitions = getPartitions(nakadiPartitions);
        return Sets.difference(currentPartitions, consumerPartitions.getPartitions());
    }

    protected Set<String> getPartitionsToRevoke(final EventTypePartitions consumerPartitions,
            final Collection<NakadiPartition> nakadiPartitions) {
        final Set<String> currentPartitions = getPartitions(nakadiPartitions);
        return Sets.difference(consumerPartitions.getPartitions(), currentPartitions);
    }

    protected Set<String> getPartitions(final Collection<NakadiPartition> nakadiPartitions) {
        return nakadiPartitions.stream().map(NakadiPartition::getPartition).collect(Collectors.toSet());
    }

    protected void revokePartition(final EventType eventType, final String partitionsToRevoke) {
        revokePartitions(eventType, Collections.singleton(partitionsToRevoke));
    }

    protected void revokePartitions(final EventType eventType, final Set<String> partitionsToRevoke) {
        if (!partitionsToRevoke.isEmpty()) {
            final PartitionRebalanceListener listener = rebalanceListeners.get(eventType);
            Preconditions.checkState(null != listener, "PartitionRebalanceListener for [%s] is not registered",
                eventType);

            final List<EventTypePartition> partitions = partitionsToRevoke.stream().map(entry ->
                        EventTypePartition.of(eventType, entry)).collect(Collectors.toList());
            listener.onPartitionsRevoked(partitions);
        }
    }

    protected void assignPartition(final EventType eventType, final NakadiPartition nakadiPartition,
            final Function<NakadiPartition, EventTypeCursor> offsetSelector) {
        final EventTypeCursor cursor = offsetSelector.apply(nakadiPartition);
        final PartitionRebalanceListener listener = rebalanceListeners.get(eventType);
        Preconditions.checkState(null != listener, "PartitionRebalanceListener for [%s] is not registered", eventType);
        listener.onPartitionsAssigned(Collections.singleton(cursor));
    }

    protected void assignPartitions(final EventType eventType, final Set<String> partitionsToAssign,
            final Collection<NakadiPartition> nakadiPartitions,
            final Function<NakadiPartition, EventTypeCursor> offsetSelector) {
        if (!partitionsToAssign.isEmpty()) {
            final List<EventTypeCursor> cursors = nakadiPartitions.stream().filter(entry ->
                                                                          partitionsToAssign.contains(
                                                                              entry.getPartition())).map(offsetSelector)
                                                                  .collect(Collectors.toList());
            final PartitionRebalanceListener listener = rebalanceListeners.get(eventType);
            Preconditions.checkState(null != listener, "PartitionRebalanceListener for [%s] is not registered",
                eventType);
            listener.onPartitionsAssigned(cursors);
        } else {
            final PartitionRebalanceListener listener = rebalanceListeners.get(eventType);
            if (null != listener) {
                listener.onPartitionsHealthCheck();
            }
        }
    }

    @Override
    public PartitionCommitCallback getPartitionCommitCallback(final EventTypePartition partition) {
        return commitCallbacks.get(partition);
    }

    @Override
    public PartitionRebalanceListener getPartitionRebalanceListener(final EventType eventType) {
        return rebalanceListeners.get(eventType);
    }
}
