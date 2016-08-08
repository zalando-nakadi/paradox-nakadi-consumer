package de.zalando.paradox.nakadi.consumer.core;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;

public interface EventHandler<T> {
    void onEvent(final EventTypeCursor cursor, final T t);
}
