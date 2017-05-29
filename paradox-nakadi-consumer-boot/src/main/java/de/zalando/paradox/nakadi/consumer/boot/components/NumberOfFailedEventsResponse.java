package de.zalando.paradox.nakadi.consumer.boot.components;

public class NumberOfFailedEventsResponse {

    private long numberOfFailedEvents;

    public NumberOfFailedEventsResponse(final long numberOfFailedEvents) {
        this.numberOfFailedEvents = numberOfFailedEvents;
    }

    public long getNumberOfFailedEvents() {
        return numberOfFailedEvents;
    }

    public void setNumberOfFailedEvents(final long numberOfFailedEvents) {
        this.numberOfFailedEvents = numberOfFailedEvents;
    }
}
