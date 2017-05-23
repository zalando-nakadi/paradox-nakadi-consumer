package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static de.zalando.paradox.nakadi.consumer.sqserrorhandler.SQSConfiguration.DEFAULT_SQS_PROPERTIES_PREFIX;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;

import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(value = "enabled", prefix = DEFAULT_SQS_PROPERTIES_PREFIX, havingValue = "true")
@ConfigurationProperties(DEFAULT_SQS_PROPERTIES_PREFIX)
public class SQSConfiguration {

    static final String DEFAULT_SQS_PROPERTIES_PREFIX = "paradox.nakadi.errorHandler.sqs";

    private String queueName;

    private String region;

    private boolean enabled;

    private boolean createQueueIfNotExists;

    private String messageVisibilityTimeout;

    private String messageRetentionPeriod;

    public String getQueueName() {
        return queueName;
    }

    public String getRegion() {
        return region;
    }

    public void setQueueName(final String queueName) {
        this.queueName = queueName;
    }

    public void setRegion(final String region) {
        this.region = region;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isCreateQueueIfNotExists() {
        return createQueueIfNotExists;
    }

    public void setCreateQueueIfNotExists(final boolean createQueueIfNotExists) {
        this.createQueueIfNotExists = createQueueIfNotExists;
    }

    public String getMessageRetentionPeriod() {
        return messageRetentionPeriod;
    }

    public void setMessageRetentionPeriod(final String messageRetentionPeriod) {
        this.messageRetentionPeriod = messageRetentionPeriod;
    }

    public String getMessageVisibilityTimeout() {
        return messageVisibilityTimeout;
    }

    public void setMessageVisibilityTimeout(final String messageVisibilityTimeout) {
        this.messageVisibilityTimeout = messageVisibilityTimeout;
    }
}
