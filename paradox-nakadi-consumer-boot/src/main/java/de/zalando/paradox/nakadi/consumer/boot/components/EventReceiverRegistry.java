package de.zalando.paradox.nakadi.consumer.boot.components;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.boot.EventReceiverRegistryConfiguration;
import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;
import de.zalando.paradox.nakadi.consumer.core.ConsumerConfig;
import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.EventStreamConfig;
import de.zalando.paradox.nakadi.consumer.core.http.HttpReactiveReceiver;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawContentHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.requests.HttpGetPartitionsHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class EventReceiverRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventReceiverRegistry.class);

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final ConcurrentMap<EventTypeConsumer, HttpReactiveReceiver> receiverMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<EventTypeConsumer, EventHandler<?>> handlerMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Pair<String, String>, PartitionCoordinator> coordinatorMap = new ConcurrentHashMap<>();

    private final EventReceiverRegistryConfiguration config;
    private final String nakadiUrl;
    private final ConsumerPartitionCoordinatorProvider coordinatorProvider;
    private final List<ConsumerEventConfig> consumerEventConfigList;
    private final AuthorizationValueProvider authorizationValueProvider;
    private final ObjectMapper objectMapper;

    public EventReceiverRegistry(final EventReceiverRegistryConfiguration config,
            @Nullable final ObjectMapper objectMapper) {
        this.config = config;
        this.nakadiUrl = requireNonNull(config.getNakadiUrl(), "nakadiUrl must not be null");
        this.coordinatorProvider = requireNonNull(config.getCoordinatorProvider(),
                "coordinatorProvider must not be null");
        this.consumerEventConfigList = requireNonNull(config.getConsumerEventConfigList().getList(),
                "consumerEventConfigList must not be null");
        this.authorizationValueProvider = config.getAuthorizationValueProvider();
        this.objectMapper = objectMapper;

    }

    @PostConstruct
    public void init() {
        if (consumerEventConfigList.isEmpty()) {
            LOGGER.warn("Nakadi event receivers are not configured");
        } else {
            consumerEventConfigList.forEach(this::startReceiver);
        }
    }

    private void startReceiver(final ConsumerEventConfig consumerEventConfig) {
        startReceiver(consumerEventConfig.getConsumerName(), consumerEventConfig.getEventName(),
            consumerEventConfig.getHandler());
    }

    private void startReceiver(final String consumerName, final String eventName, final EventHandler<?> handler) {
        checkArgument(StringUtils.isNotEmpty(consumerName), "consumerName must not be empty");
        checkArgument(StringUtils.isNotEmpty(eventName), "eventName must not be empty");
        checkArgument(null != handler, "handler must not be null");

        final EventTypeConsumer etc = new EventTypeConsumer(eventName, consumerName);
        final PartitionCoordinator coordinator = getPartitionCoordinator(consumerName, eventName);
        ConsumerConfig.Builder builder = ConsumerConfig.Builder.of(nakadiUrl, eventName, coordinator);
        if (null != authorizationValueProvider) {
            builder = builder.withAuthorization(authorizationValueProvider);
        }

        if (null != objectMapper) {
            builder = builder.withObjectMapper(objectMapper);
        }

        if (null != config.getEventsRetryAfterMillis()) {
            builder = builder.withEventsRetryAfterMillis(config.getEventsRetryAfterMillis());
        }

        if (null != config.getEventsTimeoutMillis()) {
            builder = builder.withEventsTimeoutMillis(config.getEventsTimeoutMillis());
        }

        if (null != config.getEventsRetryRandomMillis()) {
            builder = builder.withEventsRetryRandomMillis(config.getEventsRetryRandomMillis());
        }

        if (null != config.getPartitionsRetryAfterMillis()) {
            builder = builder.withPartitionsRetryAfterMillis(config.getPartitionsRetryAfterMillis());
        }

        if (null != config.getPartitionsTimeoutMillis()) {
            builder = builder.withPartitionsTimeoutMillis(config.getPartitionsTimeoutMillis());
        }

        if (null != config.getPartitionsRetryRandomMillis()) {
            builder = builder.withPartitionsRetryRandomMillis(config.getPartitionsRetryRandomMillis());
        }

        //J-
        builder.withEventStreamConfig(EventStreamConfig.Builder.of().
                withBatchLimit(config.getEventsBatchLimit()).
                withBatchTimeoutSeconds(config.getEventsBatchTimeoutSeconds()).
                withStreamLimit(config.getEventsStreamLimit()).
                withStreamTimeoutSeconds(config.getEventsStreamTimeoutSeconds()).
                withStreamKeepAliveLimit(config.getEventsStreamKeepAliveLimit()).build());
        //J+

        final ConsumerConfig config = withEventHandler(builder, handler).build();
        final HttpReactiveReceiver receiver = new HttpReactiveReceiver(new HttpGetPartitionsHandler(config));
        checkState(null == receiverMap.putIfAbsent(etc, receiver), "Duplicated configuration for [%s]", etc);
        handlerMap.putIfAbsent(etc, handler);

        LOGGER.info("Starting receiver for consumerName [{}] for event type [{}]", consumerName, config.getEventType());
        receiver.init();
    }

    private ConsumerConfig.Builder withEventHandler(final ConsumerConfig.Builder builder,
            final EventHandler<?> handler) {

        // only one interface must be implemented
        if (handler instanceof BatchEventsHandler) {
            builder.withBatchEventsHandler((BatchEventsHandler<?>) handler);
        }

        if (handler instanceof BatchEventsBulkHandler) {
            builder.withBatchEventsBulkHandler((BatchEventsBulkHandler<?>) handler);
        }

        if (handler instanceof RawContentHandler) {
            builder.withRawContentHandler((RawContentHandler) handler);
        }

        if (handler instanceof RawEventHandler) {
            builder.withRawEventHandler((RawEventHandler) handler);
        }

        if (handler instanceof RawEventBulkHandler) {
            builder.withRawEventBulkHandler((RawEventBulkHandler) handler);
        }

        if (handler instanceof JsonEventHandler) {
            builder.withJsonEventHandler((JsonEventHandler) handler);
        }

        if (handler instanceof JsonEventBulkHandler) {
            builder.withJsonEventBulkHandler((JsonEventBulkHandler) handler);
        }

        return builder;
    }

    private PartitionCoordinator getPartitionCoordinator(final String consumerName, final String eventName) {
        final Pair<String, String> key = config.isEventTypePartitionCoordinator() ? Pair.of(consumerName, eventName)
                                                                                  : Pair.of(consumerName, consumerName);
        PartitionCoordinator coordinator = coordinatorMap.get(key);
        if (null == coordinator) {
            coordinator = coordinatorProvider.getPartitionCoordinator(consumerName);

            PartitionCoordinator oldCoordinator = coordinatorMap.putIfAbsent(key, coordinator);
            if (null == oldCoordinator) {
                coordinator.init();
            } else {
                coordinator = oldCoordinator;
            }
        }

        return coordinator;
    }

    @PreDestroy
    public void destroy() throws IOException, InterruptedException {
        running.set(false);

        stop();
    }

    public void stop() throws InterruptedException {

        coordinatorMap.values().forEach(coordinator -> {
            try {
                coordinator.close();
            } catch (Exception e) {
                LOGGER.error("Unexpected error while closing coordinator", e);
            }
        });

        receiverMap.values().forEach(receiver -> {
            try {
                receiver.close();
            } catch (Exception e) {
                LOGGER.error("Unexpected error while closing receiver", e);
            }
        });

        // wait for ZK background tasks
        Thread.sleep(2000);

    }

    public void restart() {
        if (running.get()) {
            LOGGER.info("Restart receivers");

            receiverMap.values().forEach(receiver -> {
                try {
                    receiver.init();
                } catch (Exception e) {
                    LOGGER.error("Unexpected error while reinit receiver", e);
                }
            });

            coordinatorMap.values().forEach(coordinator -> {
                try {
                    coordinator.init();
                } catch (Exception e) {
                    LOGGER.error("Unexpected error while reinit coordinator", e);
                }
            });
        } else {
            LOGGER.warn("Restart receivers not possible as not running");
        }
    }

    public Set<EventTypeConsumer> getEventTypeConsumers() {
        return Collections.unmodifiableSet(handlerMap.keySet());
    }

    public EventHandler<?> getEventTypeConsumerHandler(final EventTypeConsumer eventTypeConsumer) {
        return handlerMap.get(eventTypeConsumer);
    }
}
