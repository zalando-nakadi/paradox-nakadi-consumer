package de.zalando.paradox.nakadi.consumer.boot;

import static de.zalando.paradox.nakadi.consumer.boot.SQSConfiguration.DEFAULT_SQS_PROPERTIES_PREFIX;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;

import org.springframework.stereotype.Component;

/**
 * SQS logging configuration class. If the Amazon sqs sdk is not in the classpath or if the enabled flag is not true,
 * neither the configurations will be loaded nor the error handler will be added to the context.
 */
@Component
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

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(final String region) {
        this.region = region;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(final String queueName) {
        this.queueName = queueName;
    }

}
