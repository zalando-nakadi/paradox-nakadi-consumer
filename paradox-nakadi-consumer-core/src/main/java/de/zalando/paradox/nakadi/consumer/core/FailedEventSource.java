package de.zalando.paradox.nakadi.consumer.core;

import java.util.Optional;

import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;

/**
 * Failed event source.
 */
public interface FailedEventSource<T extends FailedEvent> {

    /**
     * It will return only one failed event if it exists in the source.
     *
     * @return  Optional<T>
     */
    Optional<T> getFailedEvent();

    /**
     * It will remove the event from source.
     *
     * @param  t  is a instance of FailedEvent
     */
    void commit(T t);

    /**
     * It will return event source name.
     *
     * @return  String
     */
    String getEventSourceName();

    /**
     * It will return the size of the failed events from the source.
     *
     * @return  long
     */
    long getSize();
}
