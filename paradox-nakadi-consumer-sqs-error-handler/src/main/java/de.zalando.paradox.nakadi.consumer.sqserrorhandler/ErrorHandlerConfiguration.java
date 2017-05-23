package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNumeric;

import static com.google.common.base.Preconditions.checkArgument;

import static de.zalando.paradox.nakadi.consumer.sqserrorhandler.SQSConfiguration.DEFAULT_SQS_PROPERTIES_PREFIX;

import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import com.amazonaws.regions.Regions;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

import com.fasterxml.jackson.databind.ObjectMapper;

@ConditionalOnClass(AmazonSQS.class)
@ConditionalOnProperty(value = "enabled", prefix = DEFAULT_SQS_PROPERTIES_PREFIX, havingValue = "true")
@Configuration
public class ErrorHandlerConfiguration {

    @Lazy
    @Autowired
    private AmazonSQS amazonSQS;

    @Lazy
    @Autowired
    private SQSConfiguration sqsConfiguration;

    @Bean
    public AmazonSQS amazonSQS() {
        final AmazonSQSClientBuilder amazonSQSClientBuilder = AmazonSQSClientBuilder.standard();
        amazonSQSClientBuilder.setCredentials(new ProfileCredentialsProvider());
        amazonSQSClientBuilder.setRegion(sqsConfiguration.getRegion());
        return amazonSQSClientBuilder.build();
    }

    @ConditionalOnMissingBean
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public SQSErrorHandler sqsErrorHandler(final ObjectMapper objectMapper) {
        return new SQSErrorHandler(sqsConfiguration, amazonSQS, objectMapper);
    }

    @PostConstruct
    public void init() {

        if (sqsConfiguration.isCreateQueueIfNotExists()) {

            validateCreateQueueAttributes(sqsConfiguration);

            if (shouldCreateANewQueue(sqsConfiguration.getQueueName())) {
                final CreateQueueRequest createQueueRequest = new CreateQueueRequest();
                createQueueRequest.setQueueName(sqsConfiguration.getQueueName());
                createQueueRequest.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(),
                    sqsConfiguration.getMessageVisibilityTimeout());
                createQueueRequest.addAttributesEntry(QueueAttributeName.MessageRetentionPeriod.toString(),
                    sqsConfiguration.getMessageVisibilityTimeout());

                final CreateQueueResult createQueueResult = amazonSQS.createQueue(createQueueRequest);

                if (createQueueResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
                    throw new IllegalStateException(String.format("The queue [%s] could not be created in region [%s]",
                            sqsConfiguration.getQueueName(), sqsConfiguration.getRegion()));
                }
            }
        }
    }

    private void validateCreateQueueAttributes(final SQSConfiguration sqsConfiguration) {
        checkArgument(isNumeric(sqsConfiguration.getMessageRetentionPeriod()),
            "messageRetentionPeriod parameter must be numeric.");

        final Integer messageRetentionPeriod = Integer.valueOf(sqsConfiguration.getMessageRetentionPeriod());
        checkArgument(messageRetentionPeriod > 59 && messageRetentionPeriod < 1209601,
            "messageRetentionPeriod parameter must be between [60,1209600]");

        checkArgument(isNumeric(sqsConfiguration.getMessageVisibilityTimeout()),
            "messageVisibilityTimeout parameter must be numeric.");

        final Integer messageVisibilityTimeout = Integer.valueOf(sqsConfiguration.getMessageVisibilityTimeout());
        checkArgument(messageVisibilityTimeout >= 0 && messageVisibilityTimeout < 43200,
            "messageRetentionPeriod parameter must be between [0,43200]");

        checkArgument(isNotBlank(sqsConfiguration.getQueueName()), "queueName parameter must not be empty.");
        checkArgument(isNotBlank(sqsConfiguration.getRegion()), "region parameter must not be empty.");
        checkArgument(Arrays.stream(Regions.values()).anyMatch(region ->
                    region.getName().equals(sqsConfiguration.getRegion())), "region parameter must be valid.");
    }

    private boolean shouldCreateANewQueue(final String queueName) {
        try {
            amazonSQS.getQueueUrl(queueName);
            return false;
        } catch (final QueueDoesNotExistException queueDoesNotExistException) {
            return true;
        }
    }
}
