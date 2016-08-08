package de.zalando.paradox.nakadi.consumer.boot.components;

import com.google.common.base.MoreObjects;
import de.zalando.paradox.nakadi.consumer.core.EventHandler;

import static java.util.Objects.requireNonNull;

public class ConsumerEventConfig {

    private final String consumerName;
    private final String eventName;
    private final EventHandler<?> handler;

    public ConsumerEventConfig(final String consumerName, final String eventName, final EventHandler<?> handler) {
        this.consumerName = requireNonNull(consumerName, "baseUri must not be null");
        this.eventName = requireNonNull(eventName, "eventName must not be null");
        this.handler = requireNonNull(handler, "handler must not be null");
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getEventName() {
        return eventName;
    }

    public EventHandler<?> getHandler() {
        return handler;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("consumerName", consumerName)
                .add("eventName", eventName)
                .add("handler", handler)
                .toString();
    }
}
