package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNumeric;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.regions.Regions;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

public class SQSQueueHelper {

    private final SQSConfiguration sqsConfiguration;

    private final AmazonSQS amazonSQS;

    @Autowired
    public SQSQueueHelper(final SQSConfiguration sqsConfiguration, final AmazonSQS amazonSQS) {
        this.sqsConfiguration = sqsConfiguration;
        this.amazonSQS = amazonSQS;
    }

    @PostConstruct
    public void init() {

        if (sqsConfiguration.isCreateQueueIfNotExists()) {

            validateCreateQueueAttributes(sqsConfiguration);

            if (shouldCreateANewQueue(sqsConfiguration.getQueueName())) {

                final CreateQueueResult createQueueResult = amazonSQS.createQueue(getCreateQueueRequest());

                if (createQueueResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
                    throw new IllegalStateException(String.format("The queue [%s] could not be created in region [%s]",
                            sqsConfiguration.getQueueName(), sqsConfiguration.getRegion()));
                }
            }
        }
    }

    private CreateQueueRequest getCreateQueueRequest() {
        final CreateQueueRequest createQueueRequest = new CreateQueueRequest();
        createQueueRequest.setQueueName(sqsConfiguration.getQueueName());
        createQueueRequest.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(),
            sqsConfiguration.getMessageVisibilityTimeout());
        createQueueRequest.addAttributesEntry(QueueAttributeName.MessageRetentionPeriod.toString(),
            sqsConfiguration.getMessageVisibilityTimeout());
        return createQueueRequest;
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
