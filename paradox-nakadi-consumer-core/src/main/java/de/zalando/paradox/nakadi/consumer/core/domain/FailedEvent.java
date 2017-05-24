package de.zalando.paradox.nakadi.consumer.core.domain;

public class FailedEvent {

    private String consumerName;

    private String offset;

    private EventType eventType;

    private String partition;

    private Throwable throwable;

    private String rawEvent;

    private long failedTimeInMilliSeconds;

    private FailedEvent(final Builder builder) {
        this.consumerName = builder.consumerName;
        this.offset = builder.offset;
        this.eventType = builder.eventType;
        this.partition = builder.partition;
        this.throwable = builder.throwable;
        this.rawEvent = builder.rawEvent;
        this.failedTimeInMilliSeconds = builder.failedTimeInMilliSeconds;
    }

    public static class Builder {

        private String consumerName;

        private String offset;

        private EventType eventType;

        private String partition;

        private Throwable throwable;

        private String rawEvent;

        private Long failedTimeInMilliSeconds;

        public Builder consumerName(final String consumerName) {
            this.consumerName = consumerName;
            return this;
        }

        public Builder offset(final String offset) {
            this.offset = offset;
            return this;
        }

        public Builder eventType(final EventType eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder partition(final String partition) {
            this.partition = partition;
            return this;
        }

        public Builder throwable(final Throwable throwable) {
            this.throwable = throwable;
            return this;
        }

        public Builder rawEvent(final String rawEvent) {
            this.rawEvent = rawEvent;
            return this;
        }

        public Builder failedTimeInMilliSeconds(final long failedTimeInMilliSeconds) {
            this.failedTimeInMilliSeconds = failedTimeInMilliSeconds;
            return this;
        }

        public FailedEvent build() {
            return new FailedEvent(this);
        }

    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getOffset() {
        return offset;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getPartition() {
        return partition;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public String getRawEvent() {
        return rawEvent;
    }

    public long getFailedTimeInMilliSeconds() {
        return failedTimeInMilliSeconds;
    }
}
