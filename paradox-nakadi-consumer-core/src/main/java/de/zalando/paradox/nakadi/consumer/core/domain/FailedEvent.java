package de.zalando.paradox.nakadi.consumer.core.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FailedEvent {

    private String id;

    private String consumerName;

    private String offset;

    private EventType eventType;

    private String partition;

    private String stackTrace;

    private String rawEvent;

    private long failedTimeInMilliSeconds;

    public FailedEvent() { }

    public FailedEvent(final FailedEvent failedEvent) {
        this.id = failedEvent.getId();
        this.consumerName = failedEvent.getConsumerName();
        this.offset = failedEvent.getOffset();
        this.eventType = failedEvent.getEventType();
        this.partition = failedEvent.getPartition();
        this.stackTrace = failedEvent.getStackTrace();
        this.rawEvent = failedEvent.getRawEvent();
        this.failedTimeInMilliSeconds = failedEvent.getFailedTimeInMilliSeconds();
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(final String consumerName) {
        this.consumerName = consumerName;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(final String offset) {
        this.offset = offset;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(final EventType eventType) {
        this.eventType = eventType;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(final String partition) {
        this.partition = partition;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(final String stackTrace) {
        this.stackTrace = stackTrace;
    }

    public String getRawEvent() {
        return rawEvent;
    }

    public void setRawEvent(final String rawEvent) {
        this.rawEvent = rawEvent;
    }

    public long getFailedTimeInMilliSeconds() {
        return failedTimeInMilliSeconds;
    }

    public void setFailedTimeInMilliSeconds(final long failedTimeInMilliSeconds) {
        this.failedTimeInMilliSeconds = failedTimeInMilliSeconds;
    }

    @Override
    public String toString() {
        return "FailedEvent{" + "id='" + id + '\'' + ", consumerName='" + consumerName + '\'' + ", offset='" + offset
                + '\'' + ", eventType=" + eventType + ", partition='" + partition + '\'' + ", stackTrace='" + stackTrace
                + '\'' + ", rawEvent='" + rawEvent + '\'' + ", failedTimeInMilliSeconds=" + failedTimeInMilliSeconds
                + '}';
    }

}
