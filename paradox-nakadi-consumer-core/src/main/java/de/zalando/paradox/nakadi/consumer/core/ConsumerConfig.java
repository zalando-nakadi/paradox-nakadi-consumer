package de.zalando.paradox.nakadi.consumer.core;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Preconditions;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsResponseBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsResponseHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventResponseBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventResponseHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawContentHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawContentResponseHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventResponseBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventResponseHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.ResponseHandlerFactory;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class ConsumerConfig {

    private final String nakadiUrl;

    private final EventType eventType;

    private final ObjectMapper objectMapper;

    private final long eventsTimeoutMillis;

    private final long eventsRetryAfterMillis;

    private final long eventsRetryRandomMillis;

    private final EventStreamConfig eventStreamConfig;

    private final long partitionsTimeoutMillis;

    private final long partitionsRetryRandomMillis;

    private final long partitionsRetryAfterMillis;

    private final AuthorizationValueProvider authorizationValueProvider;

    private final PartitionCoordinator partitionCoordinator;

    private final ResponseHandlerFactory responseHandlerFactory;

    private final String consumerName;

    private ConsumerConfig(final Builder builder) {
        this.nakadiUrl = requireNonNull(builder.nakadiUrl, "nakadiUrl must not be null");
        this.eventType = requireNonNull(builder.eventType, "eventType must not be null");
        this.objectMapper = requireNonNull(builder.objectMapper, "objectMapper must not be null");
        this.eventsTimeoutMillis = builder.eventsTimeoutMillis;
        this.eventsRetryAfterMillis = builder.eventsRetryAfterMillis;
        this.eventsRetryRandomMillis = builder.eventsRetryRandomMillis;
        this.eventStreamConfig = builder.eventStreamConfig;
        this.partitionsTimeoutMillis = builder.partitionsTimeoutMillis;
        this.partitionsRetryAfterMillis = builder.partitionsRetryAfterMillis;
        this.partitionsRetryRandomMillis = builder.partitionsRetryRandomMillis;
        this.authorizationValueProvider = builder.authorizationValueProvider;
        this.partitionCoordinator = requireNonNull(builder.partitionCoordinator,
                "partitionCoordinator must not be null");
        this.responseHandlerFactory = requireNonNull(builder.responseHandlerFactory,
                "responseHandlerFactory must not be null");
        this.consumerName = requireNonNull(builder.consumerName, "consumerName must not be null");
    }

    public String getNakadiUrl() {
        return nakadiUrl;
    }

    public EventType getEventType() {
        return eventType;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public long getEventsTimeoutMillis() {
        return eventsTimeoutMillis;
    }

    public long getEventsRetryAfterMillis() {
        return eventsRetryAfterMillis;
    }

    public long getEventsRetryRandomMillis() {
        return eventsRetryRandomMillis;
    }

    public long getPartitionsTimeoutMillis() {
        return partitionsTimeoutMillis;
    }

    public long getPartitionsRetryAfterMillis() {
        return partitionsRetryAfterMillis;
    }

    public long getPartitionsRetryRandomMillis() {
        return partitionsRetryRandomMillis;
    }

    public AuthorizationValueProvider getAuthorizationValueProvider() {
        return authorizationValueProvider;
    }

    public PartitionCoordinator getPartitionCoordinator() {
        return partitionCoordinator;
    }

    public ResponseHandlerFactory getResponseHandlerFactory() {
        return responseHandlerFactory;
    }

    public EventStreamConfig getEventStreamConfig() {
        return eventStreamConfig;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public static class Builder {

        private final String nakadiUrl;

        private final EventType eventType;

        private ObjectMapper objectMapper = defaultObjectMapper();

        private long eventsTimeoutMillis = TimeUnit.SECONDS.toMillis(120);

        private long eventsRetryAfterMillis = TimeUnit.SECONDS.toMillis(2);

        private long eventsRetryRandomMillis = TimeUnit.SECONDS.toMillis(0);

        private EventStreamConfig eventStreamConfig;

        private long partitionsTimeoutMillis = TimeUnit.SECONDS.toMillis(10);

        private long partitionsRetryAfterMillis = TimeUnit.SECONDS.toMillis(15);

        private long partitionsRetryRandomMillis = TimeUnit.SECONDS.toMillis(5);

        private AuthorizationValueProvider authorizationValueProvider;

        private final PartitionCoordinator partitionCoordinator;

        private ResponseHandlerFactory responseHandlerFactory;

        private final String consumerName;

        public Builder(final String nakadiUrl, final String eventName, final PartitionCoordinator partitionCoordinator,
                final String consumerName) {
            this.nakadiUrl = nakadiUrl;
            this.eventType = EventType.of(requireNonNull(eventName, "eventName must not be null"));
            this.partitionCoordinator = partitionCoordinator;
            this.consumerName = consumerName;
        }

        public static Builder of(final String nakadiUrl, final String eventName, final PartitionCoordinator coordinator,
                final String consumerName) {
            return new Builder(nakadiUrl, eventName, coordinator, consumerName);
        }

        public Builder withObjectMapper(final ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public Builder withAuthorization(final AuthorizationValueProvider authorizationValueProvider) {
            this.authorizationValueProvider = authorizationValueProvider;
            return this;
        }

        public Builder withEventsTimeoutMillis(final long eventsTimeoutMillis) {
            this.eventsTimeoutMillis = eventsTimeoutMillis;
            return this;
        }

        public Builder withEventsRetryAfterMillis(final long eventsRetryAfterMillis) {
            this.eventsRetryAfterMillis = eventsRetryAfterMillis;
            return this;
        }

        public Builder withEventsRetryRandomMillis(final long eventsRetryRandomMillis) {
            this.eventsRetryRandomMillis = eventsRetryRandomMillis;
            return this;
        }

        public Builder withEventStreamConfig(final EventStreamConfig eventStreamConfig) {
            this.eventStreamConfig = eventStreamConfig;
            return this;
        }

        public Builder withPartitionsTimeoutMillis(final long partitionsTimeoutMillis) {
            this.partitionsTimeoutMillis = partitionsTimeoutMillis;
            return this;
        }

        public Builder withPartitionsRetryAfterMillis(final long partitionsRetryAfterMillis) {
            this.partitionsRetryAfterMillis = partitionsRetryAfterMillis;
            return this;
        }

        public Builder withPartitionsRetryRandomMillis(final long partitionsRetryRandomMillis) {
            this.partitionsRetryRandomMillis = partitionsRetryRandomMillis;
            return this;
        }

        public Builder withResponseHandlerFactory(final ResponseHandlerFactory responseHandlerFactory) {
            Preconditions.checkState(isNull(this.responseHandlerFactory),
                "responseHandlerFactory is already initialized");
            this.responseHandlerFactory = responseHandlerFactory;
            return this;
        }

        public <T> Builder withBatchEventsHandler(final BatchEventsHandler<T> handler) {
            return withResponseHandlerFactory((eventTypePartition, jsonMapper) ->
                        new BatchEventsResponseHandler<>(consumerName, eventTypePartition, jsonMapper,
                            partitionCoordinator, handler));
        }

        public <T> Builder withBatchEventsBulkHandler(final BatchEventsBulkHandler<T> handler) {
            return withResponseHandlerFactory((eventTypePartition, jsonMapper) ->
                        new BatchEventsResponseBulkHandler<>(consumerName, eventTypePartition, jsonMapper,
                            partitionCoordinator, handler));
        }

        public Builder withRawContentHandler(final RawContentHandler handler) {
            return withResponseHandlerFactory((eventTypePartition, jsonMapper) ->
                        new RawContentResponseHandler(consumerName, eventTypePartition, jsonMapper,
                            partitionCoordinator, handler));
        }

        public Builder withRawEventHandler(final RawEventHandler handler) {
            return withResponseHandlerFactory((eventTypePartition, jsonMapper) ->
                        new RawEventResponseHandler(consumerName, eventTypePartition, jsonMapper, partitionCoordinator,
                            handler));
        }

        public Builder withRawEventBulkHandler(final RawEventBulkHandler handler) {
            return withResponseHandlerFactory((eventTypePartition, jsonMapper) ->
                        new RawEventResponseBulkHandler(consumerName, eventTypePartition, jsonMapper,
                            partitionCoordinator, handler));
        }

        public Builder withJsonEventHandler(final JsonEventHandler handler) {
            return withResponseHandlerFactory((eventTypePartition, jsonMapper) ->
                        new JsonEventResponseHandler(consumerName, eventTypePartition, jsonMapper, partitionCoordinator,
                            handler));
        }

        public Builder withJsonEventBulkHandler(final JsonEventBulkHandler handler) {
            return withResponseHandlerFactory((eventTypePartition, jsonMapper) ->
                        new JsonEventResponseBulkHandler(consumerName, eventTypePartition, jsonMapper,
                            partitionCoordinator, handler));
        }

        public static ObjectMapper defaultObjectMapper() {
            return new DefaultObjectMapper().jacksonObjectMapper();
        }

        public ConsumerConfig build() {
            return new ConsumerConfig(this);
        }
    }
}
