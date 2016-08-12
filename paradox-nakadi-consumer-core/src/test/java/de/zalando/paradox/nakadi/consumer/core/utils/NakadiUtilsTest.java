package de.zalando.paradox.nakadi.consumer.core.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;
import de.zalando.paradox.nakadi.consumer.core.ConsumerConfig;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.partitioned.impl.SimplePartitionCoordinator;

public class NakadiUtilsTest {

    private static final String NAKADI_URL = "http://localhost:8080";

    @Test
    @Ignore("Needs Nakadi server")
    public void testGetNakadiPartitionsFound() {
        final ConsumerConfig config = getConfig(NAKADI_URL, "order.ORDER_RECEIVED", 5000L, null);
        final List<NakadiPartition> list = NakadiUtils.getNakadiPartitions(config).toBlocking().value();
        assertThat(list).isNotEmpty();
    }

    @Test
    @Ignore("Needs Nakadi server")
    public void testGetNakadiPartitionsNotFound() {
        final ConsumerConfig config = getConfig(NAKADI_URL, "__not_configured", 5000L, null);
        final List<NakadiPartition> list = NakadiUtils.getNakadiPartitions(config).onErrorReturn(throwable ->
                    Collections.emptyList()).toBlocking().value();
        assertThat(list).isEmpty();
    }

    private ConsumerConfig getConfig(final String nakadiUrl, final String eventName, final long partitionsTimeoutMillis,
            final AuthorizationValueProvider authorizationValueProvider) {
        final ConsumerConfig.Builder builder = ConsumerConfig.Builder.of(nakadiUrl, eventName,
                new SimplePartitionCoordinator());
        builder.withResponseHandlerFactory((eventTypePartition, jsonMapper) -> null);

        if (partitionsTimeoutMillis > 0) {
            builder.withPartitionsTimeoutMillis(partitionsTimeoutMillis);
        }

        if (authorizationValueProvider != null) {
            builder.withAuthorization(authorizationValueProvider);
        }

        return builder.build();
    }
}
