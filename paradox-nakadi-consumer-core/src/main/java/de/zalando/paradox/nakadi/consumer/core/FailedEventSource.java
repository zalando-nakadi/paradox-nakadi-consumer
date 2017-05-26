package de.zalando.paradox.nakadi.consumer.core;

import java.util.Optional;

import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;

public interface FailedEventSource<T extends FailedEvent> {

    Optional<T> getFailedEvent();

    void commit(T t);

    String getEventSourceName();

    long getApproximatelyTotalNumberOfFailedEvents();
}
