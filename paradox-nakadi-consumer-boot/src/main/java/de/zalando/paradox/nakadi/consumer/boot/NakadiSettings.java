package de.zalando.paradox.nakadi.consumer.boot;

import java.util.HashMap;
import java.util.Map;

public final class NakadiSettings {

    private final NakadiConsumerDefaults defaults = new NakadiConsumerDefaults();

    private final Map<String, NakadiConsumerSettings> consumers = new HashMap<>();

    public NakadiConsumerDefaults getDefaults() {
        return defaults;
    }

    public Map<String, NakadiConsumerSettings> getConsumers() {
        return consumers;
    }

}
