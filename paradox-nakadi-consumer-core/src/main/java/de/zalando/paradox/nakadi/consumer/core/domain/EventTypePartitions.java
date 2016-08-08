package de.zalando.paradox.nakadi.consumer.core.domain;

import java.util.Set;

import com.google.common.base.MoreObjects;

public class EventTypePartitions {
    private final EventType eventType;
    private final Set<String> partitions;

    public EventTypePartitions(final EventType eventType, final Set<String> partitions) {
        this.eventType = eventType;
        this.partitions = partitions;
    }

    public static EventTypePartitions of(final EventType eventType, final Set<String> partitions) {
        return new EventTypePartitions(eventType, partitions);
    }

    public EventType getEventType() {
        return eventType;
    }

    public Set<String> getPartitions() {
        return partitions;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("eventType", eventType).add("partitions", partitions).toString();
    }
}
