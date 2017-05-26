package de.zalando.paradox.nakadi.consumer.boot.components;

import java.util.Collection;

public class FailedEventSourceNameResponse {

    private Collection<String> failedEventSourceNames;

    public FailedEventSourceNameResponse() { }

    public FailedEventSourceNameResponse(final Collection<String> failedEventSourceNames) {
        this.failedEventSourceNames = failedEventSourceNames;
    }

    public Collection<String> getFailedEventSourceNames() {
        return failedEventSourceNames;
    }

    public void setFailedEventSourceNames(final Collection<String> failedEventSourceNames) {
        this.failedEventSourceNames = failedEventSourceNames;
    }
}
