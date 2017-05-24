package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class SQSErrorHandler implements EventErrorHandler {

    private final String queueName;

    private final AmazonSQS amazonSQS;

    private final ObjectMapper objectMapper;

    public SQSErrorHandler(final SQSConfig sqsConfig, final AmazonSQS amazonSQS, final ObjectMapper objectMapper) {
        this.amazonSQS = amazonSQS;
        this.queueName = sqsConfig.getQueueName();
        this.objectMapper = objectMapper;
    }

    @Override
    public void onError(final String consumerName, final Throwable t, final EventTypePartition eventTypePartition,
            final String offset, final String rawEvent) {

        try {

            checkArgument(isNotEmpty(consumerName), "consumerName must not be empty.");
            checkArgument(isNotEmpty(rawEvent), "rawEvent must not be empty.");
            checkArgument(isNotEmpty(offset), "offset must not be empty.");
            checkNotNull(eventTypePartition, "eventPartition must not be null.");
            checkNotNull(eventTypePartition.getEventType(), "eventTypePartition.getEventType() must not be null.");
            checkArgument(isNotEmpty(eventTypePartition.getEventType().getName()), "eventName must not be null.");
            checkArgument(isNotEmpty(eventTypePartition.getPartition()),
                "eventTypePartition.getPartition() must not be null.");
            checkNotNull(t, "exception must not be null.");

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
                throw new IllegalStateException(String.format(
                        "The result of sending event to SQS is not successful // Event body = [%s] , HttpStatusCode = [%d]",
                        serializedEvent, sendMessageResult.getSdkHttpMetadata().getHttpStatusCode()));
            }

        } catch (final Exception e) {
            ThrowableUtils.throwException(e);
        }
    }
}
