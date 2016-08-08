package de.zalando.paradox.nakadi.consumer.core.domain;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class EventTypePartition {

    private final EventType eventType;
    private final String partition;

    public EventTypePartition(final EventType eventType, final String partition) {
        this.eventType = eventType;
        this.partition = partition;
    }

    public static EventTypePartition of(final EventType eventType, final String partition) {
        return new EventTypePartition(eventType, partition);
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getName() {
        return eventType.getName();
    }

    public String getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass()).add("name", getName()).add("partition", partition)
                          .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EventTypePartition that = (EventTypePartition) o;
        return Objects.equal(eventType, that.eventType) && Objects.equal(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventType, partition);
    }
}
