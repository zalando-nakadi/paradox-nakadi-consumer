package de.zalando.paradox.nakadi.consumer.core.http.requests;

import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;

import java.io.Closeable;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;

import com.google.common.base.Preconditions;

import de.zalando.paradox.nakadi.consumer.core.ConsumerConfig;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartitions;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.http.HttpReactiveHandler;
import de.zalando.paradox.nakadi.consumer.core.http.HttpReactiveReceiver;
import de.zalando.paradox.nakadi.consumer.core.http.HttpResponseChunk;
import de.zalando.paradox.nakadi.consumer.core.http.okhttp.RxHttpRequest;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListener;
import de.zalando.paradox.nakadi.consumer.core.utils.LoggingUtils;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

import rx.Observable;

public class HttpGetPartitionsHandler implements HttpReactiveHandler, PartitionRebalanceListener, Closeable {

    private final ConcurrentMap<String, HttpReactiveReceiver> partitionReceiver = new ConcurrentHashMap<>();

    private final ConsumerConfig config;
    private final Logger log;
    private final String baseUri;
    private final EventType eventType;

    private PartitionCoordinator coordinator;
    private HttpGetPartitions httpGetPartitions;
    private final AtomicBoolean rebalanceRegistered = new AtomicBoolean(false);

    public HttpGetPartitionsHandler(final ConsumerConfig config) {
        this.config = config;
        this.log = LoggingUtils.getLogger(getClass(), config.getEventType());
        this.httpGetPartitions = new HttpGetPartitions(config.getNakadiUrl(), config.getEventType());
        this.baseUri = config.getNakadiUrl();
        this.eventType = config.getEventType();
        this.coordinator = config.getPartitionCoordinator();
    }

    @Override
    public void init() {
        if (rebalanceRegistered.compareAndSet(false, true)) {
            coordinator.registerRebalanceListener(eventType, this);
        }
    }

    @Override
    public void close() {
        if (rebalanceRegistered.compareAndSet(true, false)) {
            try {
                final List<EventTypePartition> partitions = partitionReceiver.keySet().stream().map(partition ->
                            EventTypePartition.of(eventType, partition)).collect(Collectors.toList());
                log.info("Handler close revokes partitions [{}]", partitions);
                onPartitionsRevoked(partitions);
            } finally {
                coordinator.unregisterRebalanceListener(eventType);
            }
        }
    }

    @Override
    public Logger getLogger(final Class<?> clazz) {
        return LoggingUtils.getLogger(clazz, "partitions", eventType);
    }

    @Override
    public void onResponse(final String content) {
        log.trace("ResultCallback [{}]", content);

        final Optional<List<NakadiPartition>> nakadiPartitions = getPartitions(content);
        if (nakadiPartitions.isPresent()) {
            final EventTypePartitions consumerPartitions = EventTypePartitions.of(eventType,
                    partitionReceiver.keySet());
            coordinator.rebalance(consumerPartitions, nakadiPartitions.get());
        }
    }

    private Optional<List<NakadiPartition>> getPartitions(final String content) {
        try {
            return Optional.of(config.getObjectMapper().<List<NakadiPartition>>readValue(content,
                        new TypeReference<ArrayList<NakadiPartition>>() { }));
        } catch (IOException e) {
            log.error("Error while parsing partition information", e);
            return Optional.empty();
        }
    }

    @Override
    public void onPartitionsAssigned(final Collection<EventTypeCursor> cursors) {
        log.trace("onPartitionsAssigned [{}]", cursors);
        cursors.stream().forEach(this::startReceiver);
    }

    @Override
    public void onPartitionsRevoked(final Collection<EventTypePartition> partitions) {
        log.trace("onPartitionsRevoked [{}]", partitions);
        partitions.stream().forEach(this::stopReceiver);
    }

    @Override
    public void onPartitionsHealthCheck() {
        log.trace("onPartitionsHealthCheck");
        partitionReceiver.entrySet().stream().forEach(entry -> {
            final HttpReactiveReceiver receiver = entry.getValue();
            if (receiver.isRunning() && !receiver.isSubscribed()) {

                // should never happen, only possible if an exception thrown in onNext in unhandled
                final EventTypePartition eventTypePartition = EventTypePartition.of(eventType, entry.getKey());
                try {
                    log.warn("Receiver for partition [{}] is running but unsubscribed", eventTypePartition);
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    ThrowableUtils.throwException(e);
                }

                // double check
                if (receiver.isRunning() && !receiver.isSubscribed()) {
                    log.warn("Force stop receiver for partition [{}]", eventTypePartition);
                    stopReceiver(eventTypePartition);
                }
            }
        });
    }

    private HttpReactiveReceiver stopReceiver(final EventTypePartition eventTypePartition) {
        checkEventTypePartition(eventTypePartition);

        HttpReactiveReceiver receiver = partitionReceiver.remove(eventTypePartition.getPartition());
        if (null != receiver) {
            try {
                log.info("Stopping receiver for partition [{}]", eventTypePartition);
                receiver.close();
                log.info("Receiver for partition [{}] stopped", eventTypePartition);
            } catch (Exception e) {
                log.error("Stopping receiver for partition [{}] failed due to [{}]", eventTypePartition, getMessage(e));
            }
        }

        return receiver;
    }

    private void startReceiver(final EventTypeCursor cursor) {
        checkEventTypePartition(cursor.getEventTypePartition());

        final String partition = cursor.getEventTypePartition().getPartition();
        HttpReactiveReceiver receiver = partitionReceiver.get(partition);
        if (null == receiver) {
            newReceiver(cursor);
        } else if (receiver.isRunning() && !receiver.isSubscribed()) {
            try {
                log.warn("Receiver for cursor [{}] is running but unsubscribed", cursor);
                Thread.sleep(200);
            } catch (InterruptedException e) {
                ThrowableUtils.throwException(e);
            }

            // double check
            if (receiver.isRunning() && !receiver.isSubscribed()) {
                log.warn("Force restart receiver for cursor [{}]", cursor);

                final HttpReactiveReceiver oldReceiver = stopReceiver(cursor.getEventTypePartition());
                if (receiver == oldReceiver) {
                    newReceiver(cursor);
                }
            }
        }
    }

    private void newReceiver(final EventTypeCursor cursor) {
        checkEventTypePartition(cursor.getEventTypePartition());

        final String partition = cursor.getEventTypePartition().getPartition();
        HttpReactiveReceiver receiver = null;
        try {
            receiver = new HttpReactiveReceiver(new HttpGetEventsHandler(baseUri, cursor, config));
            if (null == partitionReceiver.putIfAbsent(partition, receiver)) {
                log.info("Starting receiver for cursor [{}]", cursor);
                receiver.init();
                log.info("Receiver started for cursor [{}]", cursor);
            }
        } catch (Exception e) {
            log.error("Cannot start receiver for cursor [{}] due to [{}]", cursor, getMessage(e));
            if (null != receiver) {
                try {
                    receiver.close();
                } catch (IOException e1) {
                    log.error("Stopping receiver for cursor [{}] failed due to [{}]", cursor, getMessage(e1));
                } finally {
                    partitionReceiver.remove(partition);
                }
            }
        }
    }

    private void checkEventTypePartition(final EventTypePartition p) {
        Preconditions.checkArgument(eventType.equals(p.getEventType()), "Event type mismatch [%s]/[%s]", eventType,
            p.getEventType());
    }

    @Override
    public void onErrorResponse(final int statusCode, final String content) {
        log.trace("Error result [{} / {}]", statusCode, content);
    }

    @Override
    public void onStarted() {
        log.trace("Started");
    }

    @Override
    public void onFinished() {
        log.trace("Finished");
    }

    @Override
    public long getRetryAfterMillis() {

        // pooling time to getPartitionCommitCallback new partitions
        if (config.getPartitionsRetryRandomMillis() > 0) {
            return config.getPartitionsRetryAfterMillis()
                    + ThreadLocalRandom.current().nextLong(config.getPartitionsRetryRandomMillis());
        } else {
            return config.getPartitionsRetryAfterMillis();
        }

    }

    @Override
    public Observable<HttpResponseChunk> createRequest() {
        return new RxHttpRequest(config.getPartitionsTimeoutMillis(), config.getAuthorizationValueProvider())
                .createRequest(httpGetPartitions);
    }
}
