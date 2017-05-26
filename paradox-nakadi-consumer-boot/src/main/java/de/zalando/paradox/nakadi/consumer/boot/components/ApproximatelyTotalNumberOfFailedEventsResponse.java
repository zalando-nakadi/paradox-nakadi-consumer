package de.zalando.paradox.nakadi.consumer.boot.components;

public class ApproximatelyTotalNumberOfFailedEventsResponse {

    private long approximatelyTotalNumberOfFailedEvents;

    public ApproximatelyTotalNumberOfFailedEventsResponse(final long approximatelyTotalNumberOfFailedEvents) {
        this.approximatelyTotalNumberOfFailedEvents = approximatelyTotalNumberOfFailedEvents;
    }

    public long getApproximatelyTotalNumberOfFailedEvents() {
        return approximatelyTotalNumberOfFailedEvents;
    }

    public void setApproximatelyTotalNumberOfFailedEvents(final long approximatelyTotalNumberOfFailedEvents) {
        this.approximatelyTotalNumberOfFailedEvents = approximatelyTotalNumberOfFailedEvents;
    }
}
