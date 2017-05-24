package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomNumeric;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.http.SdkHttpMetadata;

import com.amazonaws.regions.Regions;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

public class SQSQueueHelperTest {

    @InjectMocks
    private SQSQueueHelper sqsQueueHelper;

    @Mock
    private AmazonSQS amazonSQS;

    @Mock
    private SQSConfiguration sqsConfiguration;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testShouldNotCreateQueue() {

        sqsQueueHelper.init();

        verify(sqsConfiguration).isCreateQueueIfNotExists();
        verify(sqsConfiguration, never()).getQueueName();
    }

    @Test
    public void testShouldNotAllowAlphabeticValueForMessageRetentionPeriod() {
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomAlphabetic(10));
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "messageRetentionPeriod parameter must be numeric.");
    }

    @Test
    public void testShouldNotAllowTheValueIsNotBetweenTheRangeForMessageRetentionPeriod() {
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomNumeric(1));
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "messageRetentionPeriod parameter must be between [60,1209600]");
    }

    @Test
    public void testShouldNotAllowAlphabeticValueForMessageVisibilityTimeout() {
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomNumeric(3));
        when(sqsConfiguration.getMessageVisibilityTimeout()).thenReturn(randomAlphabetic(10));

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "messageVisibilityTimeout parameter must be numeric.");
    }

    @Test
    public void testShouldNotAllowTheValueIsNotBetweenTheRangeForMessageVisibilityTimeout() {
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomNumeric(3));
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfiguration.getMessageVisibilityTimeout()).thenReturn(randomNumeric(6));

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "messageRetentionPeriod parameter must be between [0,43200]");
    }

    @Test
    public void testShouldNotAllowEmptyQueueName() {
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomNumeric(3));
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfiguration.getMessageVisibilityTimeout()).thenReturn(randomNumeric(4));

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "queueName parameter must not be empty.");
    }

    @Test
    public void testShouldNotAllowEmptyRegion() {
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomNumeric(3));
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfiguration.getMessageVisibilityTimeout()).thenReturn(randomNumeric(4));
        when(sqsConfiguration.getQueueName()).thenReturn(randomAlphabetic(10));

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "region parameter must not be empty.");
    }

    @Test
    public void testShouldNotAllowInvalidRegion() {
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomNumeric(3));
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfiguration.getMessageVisibilityTimeout()).thenReturn(randomNumeric(4));
        when(sqsConfiguration.getQueueName()).thenReturn(randomAlphabetic(10));
        when(sqsConfiguration.getRegion()).thenReturn(randomAlphabetic(18));

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "region parameter must be valid.");
    }

    @Test
    public void testShouldNotCreateANewQueueIfExists() {
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomNumeric(5));
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfiguration.getMessageVisibilityTimeout()).thenReturn(randomNumeric(4));
        when(sqsConfiguration.getQueueName()).thenReturn(randomAlphabetic(10));
        when(sqsConfiguration.getRegion()).thenReturn(Regions.EU_CENTRAL_1.getName());

        sqsQueueHelper.init();

        verify(amazonSQS, never()).createQueue(any(CreateQueueRequest.class));
    }

    @Test
    public void testShouldFailIfQueueCreationAttemptsFails() {
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomNumeric(3));
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfiguration.getMessageVisibilityTimeout()).thenReturn(randomNumeric(4));

        final String queueName = randomAlphabetic(10);
        when(sqsConfiguration.getQueueName()).thenReturn(queueName);
        when(sqsConfiguration.getRegion()).thenReturn(Regions.EU_CENTRAL_1.getName());
        when(amazonSQS.getQueueUrl(anyString())).thenThrow(QueueDoesNotExistException.class);

        final SdkHttpMetadata responseMetadata = mock(SdkHttpMetadata.class);
        when(responseMetadata.getHttpStatusCode()).thenReturn(400);

        final CreateQueueResult createQueueResult = new CreateQueueResult();
        createQueueResult.setSdkHttpMetadata(responseMetadata);

        when(amazonSQS.createQueue(any(CreateQueueRequest.class))).thenReturn(createQueueResult);

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalStateException.class).hasMessage(String
                .format("The queue [%s] could not be created in region [%s]", queueName,
                    Regions.EU_CENTRAL_1.getName()));
    }

    @Test
    public void testShouldCreateQueue() {
        when(sqsConfiguration.getMessageRetentionPeriod()).thenReturn(randomNumeric(3));
        when(sqsConfiguration.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfiguration.getMessageVisibilityTimeout()).thenReturn(randomNumeric(4));

        final String queueName = randomAlphabetic(10);
        when(sqsConfiguration.getQueueName()).thenReturn(queueName);
        when(sqsConfiguration.getRegion()).thenReturn(Regions.EU_CENTRAL_1.getName());
        when(amazonSQS.getQueueUrl(anyString())).thenThrow(QueueDoesNotExistException.class);

        final SdkHttpMetadata responseMetadata = mock(SdkHttpMetadata.class);
        when(responseMetadata.getHttpStatusCode()).thenReturn(200);

        final CreateQueueResult createQueueResult = new CreateQueueResult();
        createQueueResult.setSdkHttpMetadata(responseMetadata);

        when(amazonSQS.createQueue(any(CreateQueueRequest.class))).thenReturn(createQueueResult);

        sqsQueueHelper.init();

        verify(amazonSQS).createQueue(any(CreateQueueRequest.class));
    }
}
