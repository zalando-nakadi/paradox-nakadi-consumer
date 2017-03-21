package de.zalando.paradox.nakadi.consumer.core.client.impl;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.Iterables;

import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;
import de.zalando.paradox.nakadi.consumer.core.ConsumerConfig;
import de.zalando.paradox.nakadi.consumer.core.DefaultObjectMapper;
import de.zalando.paradox.nakadi.consumer.core.EventStreamConfig;
import de.zalando.paradox.nakadi.consumer.core.client.Client;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.http.HttpResponseChunk;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventUtils;
import de.zalando.paradox.nakadi.consumer.core.http.okhttp.RxHttpRequest;
import de.zalando.paradox.nakadi.consumer.core.http.requests.HttpGetEvents;
import de.zalando.paradox.nakadi.consumer.core.http.requests.HttpGetPartitions;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

import rx.Observable;
import rx.Single;

public class ClientImpl implements Client {

    private final String nakadiUrl;

    private final ObjectMapper objectMapper;

    private final AuthorizationValueProvider authorizationValueProvider;

    private final long partitionsTimeoutMillis;

    private final long eventsTimeoutMillis;

    public ClientImpl(final ClientImpl.Builder builder) {
        this.nakadiUrl = requireNonNull(builder.nakadiUrl, "nakadiUrl must not be null");
        this.objectMapper = requireNonNull(builder.objectMapper, "objectMapper must not be null");
        this.authorizationValueProvider = builder.authorizationValueProvider;
        this.partitionsTimeoutMillis = builder.partitionsTimeoutMillis;
        this.eventsTimeoutMillis = builder.eventsTimeoutMillis;
    }

    public ClientImpl(final ConsumerConfig consumerConfig) {
        this.nakadiUrl = consumerConfig.getNakadiUrl();
        this.objectMapper = consumerConfig.getObjectMapper();
        this.authorizationValueProvider = consumerConfig.getAuthorizationValueProvider();
        this.partitionsTimeoutMillis = consumerConfig.getPartitionsTimeoutMillis();
        this.eventsTimeoutMillis = consumerConfig.getEventsTimeoutMillis();
    }

    @Override
    public Single<List<NakadiPartition>> getPartitions(final EventType eventType) {
        final HttpGetPartitions httpGetPartitions = new HttpGetPartitions(nakadiUrl, eventType);
        final Observable<HttpResponseChunk> request = new RxHttpRequest(partitionsTimeoutMillis,
                authorizationValueProvider).createRequest(httpGetPartitions);
        return request.filter(chunk -> {
                          checkArgument(chunk.getStatusCode() == 200,
                              "Get partitions for event [%s] , result [%s / %s]", eventType, chunk.getStatusCode(),
                              chunk.getContent());
                          return true;
                      }).map(chunk -> getPartitions(chunk.getContent())).firstOrDefault(Collections.emptyList())
                      .toSingle();
    }

    private List<NakadiPartition> getPartitions(final String content) {
        try {
            return objectMapper.readValue(content, new TypeReference<ArrayList<NakadiPartition>>() { });
        } catch (IOException e) {
            ThrowableUtils.throwException(e);
            return null;
        }
    }

    @Override
    public Single<String> getEvent(final EventTypeCursor cursor) {
        final Observable<HttpResponseChunk> request = getContent0(cursor, 1);
        return request.map(chunk -> getEvent0(chunk.getContent())).firstOrDefault(null).toSingle();
    }

    @Override
    public Single<String> getContent(final EventTypeCursor cursor) {
        final Observable<HttpResponseChunk> request = getContent0(cursor, 1);
        return request.map(HttpResponseChunk::getContent).firstOrDefault(null).toSingle();
    }

    private Observable<HttpResponseChunk> getContent0(final EventTypeCursor cursor, final int streamLimit) {
        checkArgument(streamLimit > 0, "streamLimit must be greater than 0");

        final EventStreamConfig eventStreamConfig = new EventStreamConfig.Builder().withStreamLimit(streamLimit)
                                                                                   .build();
        final HttpGetEvents httpGetEvents = new HttpGetEvents(nakadiUrl, cursor, eventStreamConfig);
        return new RxHttpRequest(eventsTimeoutMillis, authorizationValueProvider).createRequest(httpGetEvents).filter(
                chunk -> {
                    checkArgument(chunk.getStatusCode() == 200, "Get for cursor [%s] , result [%s / %s]", cursor,
                        chunk.getStatusCode(), chunk.getContent());
                    checkArgument(StringUtils.isNotEmpty(chunk.getContent()), "Event not found for cursor [%s]", cursor);
                    return true;
                });
    }

    private String getEvent0(final String content) {
        final NakadiEventBatch<String> events = EventUtils.getRawEventBatch(objectMapper, content);
        checkArgument(events != null);
        return Iterables.getOnlyElement(events.getEvents());
    }

    public static class Builder {

        private final String nakadiUrl;

        private ObjectMapper objectMapper = new DefaultObjectMapper().jacksonObjectMapper();

        private long eventsTimeoutMillis = TimeUnit.SECONDS.toMillis(10);

        private long partitionsTimeoutMillis = TimeUnit.SECONDS.toMillis(10);

        private AuthorizationValueProvider authorizationValueProvider;

        public Builder(final String nakadiUrl) {
            this.nakadiUrl = nakadiUrl;
        }

        public static Builder of(final String nakadiUrl) {
            return new Builder(nakadiUrl);
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

        public Builder withPartitionsTimeoutMillis(final long partitionsTimeoutMillis) {
            this.partitionsTimeoutMillis = partitionsTimeoutMillis;
            return this;
        }

        public ClientImpl build() {
            return new ClientImpl(this);
        }
    }

    ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
