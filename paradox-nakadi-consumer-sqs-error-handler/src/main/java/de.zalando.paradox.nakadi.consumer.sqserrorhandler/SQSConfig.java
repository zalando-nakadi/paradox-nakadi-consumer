package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

public class SQSConfig {

    private String queueName;

    private String region;

    private boolean createQueueIfNotExists;

    private String messageVisibilityTimeout;

    private String messageRetentionPeriod;

    public SQSConfig(final Builder builder) {
        this.queueName = builder.queueName;
        this.region = builder.region;
        this.createQueueIfNotExists = builder.createQueueIfNotExists;
        this.messageVisibilityTimeout = builder.messageVisibilityTimeout;
        this.messageRetentionPeriod = builder.messageRetentionPeriod;
    }

    public static class Builder {

        private String queueName;

        private String region;

        private boolean createQueueIfNotExists;

        private String messageVisibilityTimeout;

        private String messageRetentionPeriod;

        public Builder queueName(final String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder region(final String region) {
            this.region = region;
            return this;
        }

        public Builder createQueueIfNotExists(final boolean createQueueIfNotExists) {
            this.createQueueIfNotExists = createQueueIfNotExists;
            return this;
        }

        public Builder messageVisibilityTimeout(final String messageVisibilityTimeout) {
            this.messageVisibilityTimeout = messageVisibilityTimeout;
            return this;
        }

        public Builder messageRetentionPeriod(final String messageRetentionPeriod) {
            this.messageRetentionPeriod = messageRetentionPeriod;
            return this;
        }

        public SQSConfig build() {
            return new SQSConfig(this);
        }
    }

    public String getQueueName() {
        return queueName;
    }

    public String getRegion() {
        return region;
    }

    public boolean isCreateQueueIfNotExists() {
        return createQueueIfNotExists;
    }

    public String getMessageRetentionPeriod() {
        return messageRetentionPeriod;
    }

    public String getMessageVisibilityTimeout() {
        return messageVisibilityTimeout;
    }
}
