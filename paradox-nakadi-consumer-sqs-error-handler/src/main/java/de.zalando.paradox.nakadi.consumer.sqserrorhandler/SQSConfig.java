package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

public class SQSConfig {

    private final String queueUrl;

    public SQSConfig(final Builder builder) {
        this.queueUrl = builder.queueUrl;
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
