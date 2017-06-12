package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class SQSErrorHandler implements EventErrorHandler {

    private final String queueName;

    private final AmazonSQS amazonSQS;

    private final ObjectMapper objectMapper;

    private final String queueUrl;

    public SQSErrorHandler(final SQSConfig sqsConfig, final AmazonSQS amazonSQS, final ObjectMapper objectMapper) {
        this.amazonSQS = amazonSQS;
        this.queueName = sqsConfig.getQueueName();
        this.objectMapper = objectMapper;
        this.queueUrl = amazonSQS.getQueueUrl(queueName).getQueueUrl();
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

            final FailedEvent failedEvent = new FailedEvent();
            failedEvent.setConsumerName(consumerName);
            failedEvent.setEventType(eventTypePartition.getEventType());
            failedEvent.setFailedTimeInMilliSeconds(System.currentTimeMillis());
            failedEvent.setOffset(offset);
            failedEvent.setPartition(eventTypePartition.getPartition());
            failedEvent.setRawEvent(rawEvent);

            try(StringWriter stackTraceStringWriter = new StringWriter();
                    PrintWriter stackTracePrintWriter = new PrintWriter(stackTraceStringWriter)) {
                t.printStackTrace(stackTracePrintWriter);
                stackTracePrintWriter.flush();
                failedEvent.setStackTrace(stackTraceStringWriter.toString());
            }

            final String serializedEvent = objectMapper.writeValueAsString(failedEvent);
            amazonSQS.sendMessage(new SendMessageRequest(queueUrl, serializedEvent));

        } catch (final Exception e) {
            ThrowableUtils.throwException(e);
        }
    }
}
