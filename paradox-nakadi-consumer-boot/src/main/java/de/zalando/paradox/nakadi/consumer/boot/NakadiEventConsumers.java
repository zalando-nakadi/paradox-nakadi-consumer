package de.zalando.paradox.nakadi.consumer.boot;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

public class NakadiEventConsumers {
    private static final NakadiEventConsumers EMPTY = new NakadiEventConsumers(Collections.emptySet());

    private final Set<NakadiEventConsumer> eventConsumers;

    public NakadiEventConsumers(final Set<NakadiEventConsumer> eventConsumers) {
        this.eventConsumers = eventConsumers;
    }

    public static NakadiEventConsumers single(final String eventName, @Nullable final String consumerName) {
        return new NakadiEventConsumers(Collections.singleton(NakadiEventConsumer.of(eventName, consumerName)));
    }

    public static NakadiEventConsumers of(final NakadiEventConsumer... eventConsumers) {
        return new NakadiEventConsumers(ImmutableSet.copyOf(eventConsumers));
    }

    public static NakadiEventConsumers empty() {
        return EMPTY;
    }

    public Set<NakadiEventConsumer> getEventConsumers() {
        return eventConsumers;
    }
}
