package de.zalando.paradox.nakadi.consumer.core;

public class EventStreamConfig {

    private final Integer batchLimit;
    private final Integer streamLimit;
    private final Integer batchTimeoutSeconds;
    private final Integer streamTimeoutSeconds;
    private final Integer streamKeepAliveLimit;

    private EventStreamConfig(final Builder builder) {
        this.batchLimit = builder.batchLimit;
        this.streamLimit = builder.streamLimit;
        this.batchTimeoutSeconds = builder.batchTimeoutSeconds;
        this.streamTimeoutSeconds = builder.streamTimeoutSeconds;
        this.streamKeepAliveLimit = builder.streamKeepAliveLimit;
    }

    public Integer getBatchLimit() {
        return batchLimit;
    }

    public Integer getStreamLimit() {
        return streamLimit;
    }

    public Integer getBatchTimeoutSeconds() {
        return batchTimeoutSeconds;
    }

    public Integer getStreamTimeoutSeconds() {
        return streamTimeoutSeconds;
    }

    public Integer getStreamKeepAliveLimit() {
        return streamKeepAliveLimit;
    }

    public static class Builder {
        private Integer batchLimit;
        private Integer streamLimit;
        private Integer batchTimeoutSeconds;
        private Integer streamTimeoutSeconds;
        private Integer streamKeepAliveLimit;

        public static Builder of() {
            return new Builder();
        }

        public Builder withBatchLimit(final Integer batchLimit) {
            this.batchLimit = batchLimit;
            return this;
        }

        public Builder withStreamLimit(final Integer streamLimit) {
            this.streamLimit = streamLimit;
            return this;
        }

        public Builder withBatchTimeoutSeconds(final Integer batchTimeoutSeconds) {
            this.batchTimeoutSeconds = batchTimeoutSeconds;
            return this;
        }

        public Builder withStreamTimeoutSeconds(final Integer streamTimeoutSeconds) {
            this.streamTimeoutSeconds = streamTimeoutSeconds;
            return this;
        }

        public Builder withStreamKeepAliveLimit(final Integer streamKeepAliveLimit) {
            this.streamKeepAliveLimit = streamKeepAliveLimit;
            return this;
        }

        public EventStreamConfig build() {
            return new EventStreamConfig(this);
        }
    }
}
