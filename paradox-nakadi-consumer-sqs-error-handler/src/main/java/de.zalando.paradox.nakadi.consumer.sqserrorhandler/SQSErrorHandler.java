package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class SQSErrorHandler implements EventErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQSErrorHandler.class);

    private final String queueName;

    private final AmazonSQS amazonSQS;

    private final ObjectMapper objectMapper;

    public SQSErrorHandler(final SQSConfiguration sqsConfiguration, final AmazonSQS amazonSQS,
            final ObjectMapper objectMapper) {
        this.amazonSQS = amazonSQS;
        this.queueName = sqsConfiguration.getQueueName();
        this.objectMapper = objectMapper;
    }

    @Override
    public void onError(final String consumerName, final Throwable t, final EventTypePartition eventTypePartition,
            final String offset, final String rawEvent) {

        try {
            final FailedEvent failedEvent = new FailedEvent.Builder().consumerName(consumerName)
                                                                     .eventType(eventTypePartition.getEventType())
                                                                     .failedTimeInMilliSeconds(System
                                                                             .currentTimeMillis()).offset(offset)
                                                                     .partition(eventTypePartition.getPartition())
                                                                     .rawEvent(rawEvent).throwable(t).build();

            final String serializedEvent = objectMapper.writeValueAsString(failedEvent);
            final GetQueueUrlResult queueUrl = amazonSQS.getQueueUrl(queueName);
            final SendMessageResult sendMessageResult = amazonSQS.sendMessage(new SendMessageRequest(
                        queueUrl.getQueueUrl(), serializedEvent));

            if (sendMessageResult.getSdkHttpMetadata().getHttpStatusCode() != 200) {
                LOGGER.error(
                    "The result of sending event to SQS is not successful // Event body = [{}] , HttpStatusCode = [{}]",
                    serializedEvent, sendMessageResult.getSdkHttpMetadata().getHttpStatusCode());
                throw new RuntimeException("The result of sending event to SQS is not successful");
            }

        } catch (final Exception e) {
            LOGGER.error("Exception occurred while sending event to SQS // Event body = [{}]", rawEvent, e);
            ThrowableUtils.throwException(e);
        }
    }
}
