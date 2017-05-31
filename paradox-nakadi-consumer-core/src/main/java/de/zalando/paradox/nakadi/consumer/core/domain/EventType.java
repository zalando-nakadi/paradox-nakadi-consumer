package de.zalando.paradox.nakadi.consumer.core.domain;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class EventType {
    private String name;

    public EventType() { }

    public EventType(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static EventType of(final String name) {
        return new EventType(name);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("name", name).toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EventType eventType = (EventType) o;
        return Objects.equal(name, eventType.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }
}
