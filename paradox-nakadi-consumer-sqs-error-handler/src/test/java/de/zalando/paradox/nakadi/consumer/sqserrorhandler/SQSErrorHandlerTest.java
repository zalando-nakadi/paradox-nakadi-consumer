package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomNumeric;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.http.SdkHttpMetadata;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;

public class SQSErrorHandlerTest {

    @InjectMocks
    private SQSErrorHandler sqsErrorHandler;

    @Mock
    private AmazonSQS amazonSQS;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private SQSConfig sqsConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testShouldVerifyTheEventCouldNotSendToSQS() throws JsonProcessingException {
        when(objectMapper.writeValueAsString(any(FailedEvent.class))).thenThrow(JsonProcessingException.class);

        assertThatThrownBy(() ->
                sqsErrorHandler.onError(randomAlphabetic(10), new RuntimeException(),
                    EventTypePartition.of(EventType.of(randomAlphabetic(10)), randomAlphabetic(1)), randomNumeric(10),
                    randomAlphabetic(50))).isInstanceOf(JsonProcessingException.class);
    }

    @Test
    public void testShouldGetFailedResponseAfterSendingTheEvent() {
        final GetQueueUrlResult getQueueUrlResult = new GetQueueUrlResult();
        getQueueUrlResult.setQueueUrl(randomAlphabetic(10));

        when(amazonSQS.getQueueUrl(anyString())).thenReturn(getQueueUrlResult);

        final SendMessageResult sendMessageResult = new SendMessageResult();

        final SdkHttpMetadata responseMetadata = mock(SdkHttpMetadata.class);
        when(responseMetadata.getHttpStatusCode()).thenReturn(400);
        sendMessageResult.setSdkHttpMetadata(responseMetadata);

        when(amazonSQS.sendMessage(any(SendMessageRequest.class))).thenReturn(sendMessageResult);

        assertThatThrownBy(() ->
                                        sqsErrorHandler.onError(randomAlphabetic(10), new RuntimeException(),
                                            EventTypePartition.of(EventType.of(randomAlphabetic(10)),
                                                randomAlphabetic(1)), randomNumeric(10), randomAlphabetic(50)))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining(
                                    "The result of sending event to SQS is not successful");
    }

    @Test
    public void testShouldSendEventToSQS() throws JsonProcessingException {
        final GetQueueUrlResult getQueueUrlResult = new GetQueueUrlResult();
        getQueueUrlResult.setQueueUrl(randomAlphabetic(10));

        when(amazonSQS.getQueueUrl(anyString())).thenReturn(getQueueUrlResult);

        final SendMessageResult sendMessageResult = new SendMessageResult();

        final SdkHttpMetadata responseMetadata = mock(SdkHttpMetadata.class);
        when(responseMetadata.getHttpStatusCode()).thenReturn(200);
        sendMessageResult.setSdkHttpMetadata(responseMetadata);

        when(amazonSQS.sendMessage(any(SendMessageRequest.class))).thenReturn(sendMessageResult);

        sqsErrorHandler.onError(randomAlphabetic(10), new RuntimeException(),
            EventTypePartition.of(EventType.of(randomAlphabetic(10)), randomAlphabetic(1)), randomNumeric(10),
            randomAlphabetic(50));

        verify(objectMapper).writeValueAsString(anyString());
        verify(amazonSQS).getQueueUrl(anyString());
        verify(amazonSQS).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    public void testShouldFailIfConsumerNameIsNull() {
        assertThatThrownBy(() -> sqsErrorHandler.onError(null, null, null, null, null)).isInstanceOf(
            IllegalArgumentException.class).hasMessage("consumerName must not be empty.");
    }

    @Test
    public void testShouldFailIfRawEventIsNull() {
        assertThatThrownBy(() -> sqsErrorHandler.onError(randomAlphabetic(10), null, null, null, null)).isInstanceOf(
            IllegalArgumentException.class).hasMessage("rawEvent must not be empty.");
    }

    @Test
    public void testShouldFailIfOffsetIsNull() {
        assertThatThrownBy(() ->
                                        sqsErrorHandler.onError(randomAlphabetic(10), null, null, null,
                                            randomAlphabetic(10))).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("offset must not be empty.");
    }

    @Test
    public void testShouldFailIfEventPartitionIsNull() {
        assertThatThrownBy(() ->
                                        sqsErrorHandler.onError(randomAlphabetic(10), null, null, randomAlphabetic(10),
                                            randomAlphabetic(10))).isInstanceOf(NullPointerException.class).hasMessage(
                                    "eventPartition must not be null.");
    }

    @Test
    public void testShouldFailIfEventTypeIsNull() {
        assertThatThrownBy(() ->
                                        sqsErrorHandler.onError(randomAlphabetic(10), null,
                                            new EventTypePartition(null, null), randomAlphabetic(10),
                                            randomAlphabetic(10))).isInstanceOf(NullPointerException.class).hasMessage(
                                    "eventTypePartition.getEventType() must not be null.");
    }

    @Test
    public void testShouldFailIfEventNameIsNull() {
        assertThatThrownBy(() ->
                                        sqsErrorHandler.onError(randomAlphabetic(10), null,
                                            new EventTypePartition(new EventType(null), null), randomAlphabetic(10),
                                            randomAlphabetic(10))).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("eventName must not be null.");
    }

    @Test
    public void testShouldFailIfPartitionIsNull() {
        assertThatThrownBy(() ->
                                        sqsErrorHandler.onError(randomAlphabetic(10), null,
                                            new EventTypePartition(new EventType(randomAlphabetic(10)), null),
                                            randomAlphabetic(10), randomAlphabetic(10))).isInstanceOf(
            IllegalArgumentException.class).hasMessage("eventTypePartition.getPartition() must not be null.");
    }

    @Test
    public void testShouldFailIfExceptionIsNull() {
        assertThatThrownBy(() ->
                                        sqsErrorHandler.onError(randomAlphabetic(10), null,
                                            new EventTypePartition(new EventType(randomAlphabetic(10)),
                                                randomAlphabetic(10)), randomAlphabetic(10), randomAlphabetic(10)))
            .isInstanceOf(NullPointerException.class).hasMessage("exception must not be null.");
    }
}
