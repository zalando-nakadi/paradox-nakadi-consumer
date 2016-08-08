package de.zalando.paradox.nakadi.consumer.boot.components;

import java.util.List;

public class ConsumerEventConfigList {
    private final List<ConsumerEventConfig> list;

    public ConsumerEventConfigList(final List<ConsumerEventConfig> list) {
        this.list = list;
    }

    public List<ConsumerEventConfig> getList() {
        return list;
    }
}
