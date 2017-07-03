package de.zalando.paradox.nakadi.consumer.core.client.impl;

import static java.lang.String.format;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;
import de.zalando.paradox.nakadi.consumer.core.DefaultObjectMapper;
import de.zalando.paradox.nakadi.consumer.core.EventStreamConfig;
import de.zalando.paradox.nakadi.consumer.core.client.Client;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.exceptions.InvalidEventTypeException;
import de.zalando.paradox.nakadi.consumer.core.http.HttpResponseChunk;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventUtils;
import de.zalando.paradox.nakadi.consumer.core.http.okhttp.RxHttpRequest;
import de.zalando.paradox.nakadi.consumer.core.http.requests.HttpGetEvents;
import de.zalando.paradox.nakadi.consumer.core.http.requests.HttpGetPartitions;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import rx.Observable;
import rx.Single;

public class ClientImpl implements Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientImpl.class);

    private static final TypeReference<List<NakadiPartition>> NAKADI_PARTITIONS_TYPE =
        new TypeReference<List<NakadiPartition>>() { };

    private static final MediaType CONTENT_TYPE = MediaType.parse("application/json");

    private final String nakadiUrl;

    private final ObjectMapper objectMapper;

    private final AuthorizationValueProvider authorizationValueProvider;

    private final long partitionsTimeoutMillis;

    private final long eventsTimeoutMillis;

    private final OkHttpClient okHttpClient = initHttpClient();

    private ClientImpl(final ClientImpl.Builder builder) {
        this.nakadiUrl = requireNonNull(builder.nakadiUrl, "nakadiUrl must not be null");
        this.objectMapper = requireNonNull(builder.objectMapper, "objectMapper must not be null");
        this.authorizationValueProvider = builder.authorizationValueProvider;
        this.partitionsTimeoutMillis = builder.partitionsTimeoutMillis;
        this.eventsTimeoutMillis = builder.eventsTimeoutMillis;
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
        try {
            return request.map(chunk -> getEvent0(chunk.getContent(), cursor.getEventType())).firstOrDefault(null)
                          .toSingle();
        } catch (InvalidEventTypeException e) {
            LOGGER.error("Invalid event type error for offset [{}] on partition [{}]: {}", cursor.getOffset(),
                cursor.getPartition(), e.getMessage());
            ThrowableUtils.throwException(e);
        }

        return null;
    }

    @Override
    public Single<String> getContent(final EventTypeCursor cursor) {
        final Observable<HttpResponseChunk> request = getContent0(cursor, 1);
        return request.map(HttpResponseChunk::getContent).firstOrDefault(null).toSingle();
    }

    @Override
    public Single<List<NakadiPartition>> getCursorsLag(final List<EventTypeCursor> cursors) {
        checkNotNull(cursors, "cursors must not be null");
        checkArgument(!cursors.isEmpty(), "cursors must not be empty");
        checkArgument(cursors.stream().map(EventTypeCursor::getEventType).distinct().count() == 1,
            "cursors must contain cursors of only one type");

        return Single.<List<NakadiPartition>>fromCallable(() -> {
                final EventType eventType = cursors.get(0).getEventType();
                final HttpUrl httpUrl = HttpUrl.parse(nakadiUrl).newBuilder().addPathSegment("event-types")
                        .addPathSegment(eventType.getName()).addPathSegment("cursors-lag").build();
                final Request request = new Request.Builder().url(httpUrl).post(
                        RequestBody.create(CONTENT_TYPE, //
                            getNakadiCursors(cursors))) //
                    .build();
                final Response response = okHttpClient.newCall(request).execute();
                if (response.isSuccessful()) {
                    return objectMapper.readValue(response.body().byteStream(), NAKADI_PARTITIONS_TYPE);
                } else {
                    throw new RuntimeException(
                        format("Get cursors lag failed for cursors [%s]: [%s]", cursors, response.body().string()));
                }
            });
    }

    private String getNakadiCursors(final List<EventTypeCursor> cursors) throws JsonProcessingException {
        final List<NakadiCursor> nakadiCursors = cursors.stream().map(cursor ->
                    new NakadiCursor(cursor.getPartition(), cursor.getOffset())).collect(Collectors.toList());
        return objectMapper.writeValueAsString(nakadiCursors);
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

    private String getEvent0(final String content, final EventType eventType) {
        final NakadiEventBatch<String> events = EventUtils.getRawEventBatch(objectMapper, content, eventType);
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

    @VisibleForTesting
    OkHttpClient initHttpClient() {
        return new OkHttpClient.Builder().addInterceptor(getAuthorizationInterceptor()).build();
    }

    private Interceptor getAuthorizationInterceptor() {
        return new Interceptor() {
            @Override
            public Response intercept(final Chain chain) throws IOException {
                if (authorizationValueProvider != null) {
                    return chain.proceed(chain.request().newBuilder().addHeader("Authorization",
                                authorizationValueProvider.get()).build());
                } else {
                    return chain.proceed(chain.request());
                }
            }
        };
    }

}
