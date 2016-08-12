package de.zalando.paradox.nakadi.consumer.core.utils;

import static com.google.common.base.Preconditions.checkArgument;

import static de.zalando.paradox.nakadi.consumer.core.http.requests.HttpGetPartitionsHandler.getPartitions;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;
import de.zalando.paradox.nakadi.consumer.core.ConsumerConfig;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.http.HttpResponseChunk;
import de.zalando.paradox.nakadi.consumer.core.http.okhttp.RxHttpRequest;
import de.zalando.paradox.nakadi.consumer.core.http.requests.HttpGetPartitions;

import rx.Observable;
import rx.Single;

public class NakadiUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(NakadiUtils.class);

    public static Single<List<NakadiPartition>> getNakadiPartitions(final String nakadiUrl, final EventType eventType,
            @Nullable final AuthorizationValueProvider authorizationValueProvider, final long partitionsTimeoutMillis,
            final ObjectMapper objectMapper) {

        final HttpGetPartitions httpGetPartitions = new HttpGetPartitions(nakadiUrl, eventType);
        final Observable<HttpResponseChunk> request = new RxHttpRequest(partitionsTimeoutMillis,
                authorizationValueProvider).createRequest(httpGetPartitions);

        return request.filter(chunk -> {
                          checkArgument(chunk.getStatusCode() == 200,
                              "Get partitions for event [%s] , result [%s / %s]", eventType, chunk.getStatusCode(),
                              chunk.getContent());
                          return true;
                      }).map(chunk ->
                              getPartitions(chunk.getContent(), objectMapper, LOGGER).orElse(Collections.emptyList()))
                      .firstOrDefault(Collections.emptyList()).toSingle();

    }

    public static Single<List<NakadiPartition>> getNakadiPartitions(final ConsumerConfig config) {
        return getNakadiPartitions(config.getNakadiUrl(), config.getEventType(), config.getAuthorizationValueProvider(),
                config.getPartitionsTimeoutMillis(), config.getObjectMapper());
    }
}
