package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import javax.annotation.Nullable;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

@FunctionalInterface
public interface EventErrorHandler {

    void onError(Throwable t, EventTypePartition eventTypePartition, @Nullable String offset, String rawEvent);
}
