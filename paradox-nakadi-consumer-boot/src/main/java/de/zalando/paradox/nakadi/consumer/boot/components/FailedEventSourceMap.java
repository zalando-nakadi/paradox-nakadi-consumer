package de.zalando.paradox.nakadi.consumer.boot.components;

import java.util.HashMap;
import java.util.Map;

import de.zalando.paradox.nakadi.consumer.core.FailedEventSource;
import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;

public class FailedEventSourceMap {

    private Map<String, FailedEventSource<FailedEvent>> failedEventSourceMap;

    public FailedEventSourceMap() {
        this(new HashMap<>());
    }

    public FailedEventSourceMap(final Map<String, FailedEventSource<FailedEvent>> failedEventSourceMap) {
        this.failedEventSourceMap = failedEventSourceMap;
    }

    public Map<String, FailedEventSource<FailedEvent>> getFailedEventSourceMap() {
        return failedEventSourceMap;
    }

    public void setFailedEventSourceMap(final Map<String, FailedEventSource<FailedEvent>> failedEventSourceMap) {
        this.failedEventSourceMap = failedEventSourceMap;
    }
}
