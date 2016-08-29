package de.zalando.paradox.nakadi.consumer.boot.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class EventTypeConsumer {

    @JsonProperty("event_type")
    private final String eventName;

    @JsonProperty("consumer_name")
    private final String consumerName;

    public EventTypeConsumer(final String eventType, final String consumerName) {
        this.eventName = eventType;
        this.consumerName = consumerName;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("eventName", eventName).add("consumerName", consumerName)
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

        EventTypeConsumer that = (EventTypeConsumer) o;
        return Objects.equal(eventName, that.eventName) && Objects.equal(consumerName, that.consumerName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventName, consumerName);
    }

    public String getEventName() {
        return eventName;
    }

    public String getConsumerName() {
        return consumerName;
    }
}
