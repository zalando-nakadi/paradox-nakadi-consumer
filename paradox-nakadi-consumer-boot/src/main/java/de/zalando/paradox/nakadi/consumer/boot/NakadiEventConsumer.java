package de.zalando.paradox.nakadi.consumer.boot;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class NakadiEventConsumer {

    public final String eventName;

    public final String consumerName;

    public NakadiEventConsumer(final String eventName, @Nullable final String consumerName) {
        Preconditions.checkArgument(null != eventName && !eventName.isEmpty(), "eventName must not be empty");
        this.eventName = eventName;
        this.consumerName = consumerName;
    }

    public String getEventName() {
        return eventName;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public static NakadiEventConsumer of(final String eventName, @Nullable final String consumerName) {
        return new NakadiEventConsumer(eventName, consumerName);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NakadiEventConsumer that = (NakadiEventConsumer) o;
        return Objects.equal(eventName, that.eventName) && Objects.equal(consumerName, that.consumerName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventName, consumerName);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("eventName", eventName).add("consumerName", consumerName)
                          .toString();
    }
}
