package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import javax.annotation.Nullable;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

/**
 * It is used for failed events. Failed events are going to be caught automatically and will be given to the
 * implementations of the EventErrorHandler interface.
 */
@FunctionalInterface
public interface EventErrorHandler {

    /**
     * This callback method will be called when an exception occurred. The Exception can be many things such as a broken
     * event, unparsable event body, database connection timeout etc.
     *
     * @param  t                   Thrown exception itself
     * @param  eventTypePartition  EventTypePartition contains eventType and partition information
     * @param  offset              Current offset
     * @param  rawEvent            Raw event body itself
     */
    void onError(Throwable t, EventTypePartition eventTypePartition, @Nullable String offset, String rawEvent);
}
