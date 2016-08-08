package de.zalando.paradox.nakadi.consumer.core.domain;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class EventTypeCursor {

    private final EventTypePartition eventTypePartition;
    private final String offset;

    public EventTypeCursor(final EventTypePartition eventTypePartition, final String offset) {
        this.eventTypePartition = eventTypePartition;
        this.offset = offset;
    }

    public static EventTypeCursor of(final EventTypePartition eventTypePartition, final String offset) {
        return new EventTypeCursor(eventTypePartition, offset);
    }

    public String getOffset() {
        return offset;
    }

    public String getPartition() {
        return eventTypePartition.getPartition();
    }

    public EventTypePartition getEventTypePartition() {
        return eventTypePartition;
    }

    public EventType getEventType() {
        return eventTypePartition.getEventType();
    }

    public String getName() {
        return eventTypePartition.getName();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass()).add("name", getName()).add("partition", getPartition())
                          .add("offset", offset).toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EventTypeCursor that = (EventTypeCursor) o;
        return Objects.equal(eventTypePartition, that.eventTypePartition) && Objects.equal(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventTypePartition, offset);
    }
}
