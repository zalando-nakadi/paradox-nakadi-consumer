package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static java.util.Objects.requireNonNull;

public class SQSConfig {

    private final String queueUrl;

    public SQSConfig(final Builder builder) {
        this.queueUrl = requireNonNull(builder.queueUrl, "queueUrl must not be null");
    }

    public static class Builder {

        private String queueUrl;

        public Builder queueUrl(final String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        public SQSConfig build() {
            return new SQSConfig(this);
        }
    }

    public String getQueueUrl() {
        return queueUrl;
    }

}
