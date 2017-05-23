package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static de.zalando.paradox.nakadi.consumer.sqserrorhandler.SQSConfiguration.DEFAULT_SQS_PROPERTIES_PREFIX;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;

import org.springframework.stereotype.Component;

import com.amazonaws.services.sqs.AmazonSQS;

/**
 * SQS logging configuration class. If the Amazon sqs sdk is not in the classpath or if the enabled flag is not true,
 * neither the configurations will be loaded nor the error handler will be added to the context.
 */
@Component
@ConditionalOnClass(AmazonSQS.class)
@ConditionalOnProperty(value = "enabled", prefix = DEFAULT_SQS_PROPERTIES_PREFIX, havingValue = "true")
@ConfigurationProperties(DEFAULT_SQS_PROPERTIES_PREFIX)
public class SQSConfiguration {

    static final String DEFAULT_SQS_PROPERTIES_PREFIX = "paradox.nakadi.errorHandler.sqs";

    /**
     * SQS Queue Name.
     */
    private String queueName;

    /**
     * AWS Region.
     */
    private String region;

    /**
     * Switch for enabling sqs logging.
     */
    private boolean enabled;

    /**
     * Switch for enabling to create the queue if the given queue does not exist.
     */
    private boolean createQueueIfNotExists;

    /**
     * The length of time (in seconds) that a message received from a queue will be invisible to other receiving
     * components. The value must be between 0 and 43200
     */
    private String messageVisibilityTimeout;

    /**
     * The amount of time that Amazon SQS will retain a message if it does not get deleted. The value must be between 60
     * and 1209600.
     */
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
