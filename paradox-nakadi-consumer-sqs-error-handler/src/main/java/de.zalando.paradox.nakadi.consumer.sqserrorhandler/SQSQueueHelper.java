package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNumeric;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

import javax.annotation.PostConstruct;

import com.amazonaws.regions.Regions;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

public class SQSQueueHelper {

    private final SQSConfig sqsConfig;

    private final AmazonSQS amazonSQS;

    public SQSQueueHelper(final SQSConfig sqsConfig, final AmazonSQS amazonSQS) {
        this.sqsConfig = sqsConfig;
        this.amazonSQS = amazonSQS;
    }

    @PostConstruct
    public void init() {

        if (sqsConfig.isCreateQueueIfNotExists()) {

            validateCreateQueueAttributes(sqsConfig);

            if (shouldCreateANewQueue(sqsConfig.getQueueName())) {

                final CreateQueueResult createQueueResult = amazonSQS.createQueue(getCreateQueueRequest());

                if (createQueueResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
                    throw new IllegalStateException(String.format("The queue [%s] could not be created in region [%s]",
                            sqsConfig.getQueueName(), sqsConfig.getRegion()));
                }
            }
        }
    }

    private CreateQueueRequest getCreateQueueRequest() {
        final CreateQueueRequest createQueueRequest = new CreateQueueRequest();
        createQueueRequest.setQueueName(sqsConfig.getQueueName());
        createQueueRequest.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(),
            sqsConfig.getMessageVisibilityTimeout());
        createQueueRequest.addAttributesEntry(QueueAttributeName.MessageRetentionPeriod.toString(),
            sqsConfig.getMessageVisibilityTimeout());
        return createQueueRequest;
    }

    private void validateCreateQueueAttributes(final SQSConfig sqsConfig) {
        checkArgument(isNumeric(sqsConfig.getMessageRetentionPeriod()),
            "messageRetentionPeriod parameter must be numeric.");

        final Integer messageRetentionPeriod = Integer.valueOf(sqsConfig.getMessageRetentionPeriod());
        checkArgument(messageRetentionPeriod > 59 && messageRetentionPeriod < 1209601,
            "messageRetentionPeriod parameter must be between [60,1209600]");

        checkArgument(isNumeric(sqsConfig.getMessageVisibilityTimeout()),
            "messageVisibilityTimeout parameter must be numeric.");

        final Integer messageVisibilityTimeout = Integer.valueOf(sqsConfig.getMessageVisibilityTimeout());
        checkArgument(messageVisibilityTimeout >= 0 && messageVisibilityTimeout < 43200,
            "messageRetentionPeriod parameter must be between [0,43200]");

        checkArgument(isNotBlank(sqsConfig.getQueueName()), "queueName parameter must not be empty.");
        checkArgument(isNotBlank(sqsConfig.getRegion()), "region parameter must not be empty.");
        checkArgument(Arrays.stream(Regions.values()).anyMatch(region ->
                    region.getName().equals(sqsConfig.getRegion())), "region parameter must be valid.");
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
