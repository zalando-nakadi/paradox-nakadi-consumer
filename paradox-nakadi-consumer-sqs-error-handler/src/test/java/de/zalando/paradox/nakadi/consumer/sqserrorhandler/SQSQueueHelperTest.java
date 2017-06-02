package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.http.SdkHttpMetadata;

import com.amazonaws.regions.Regions;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

public class SQSQueueHelperTest {

    @InjectMocks
    private SQSQueueHelper sqsQueueHelper;

    @Mock
    private AmazonSQS amazonSQS;

    @Mock
    private SQSConfig sqsConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testShouldNotCreateQueue() {
        sqsQueueHelper.init();

        verify(sqsConfig).isCreateQueueIfNotExists();
        verify(sqsConfig, never()).getQueueName();
    }

    @Test
    public void testShouldNotAllowAlphabeticValueForMessageRetentionPeriod() {
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("invalid");

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "messageRetentionPeriod parameter must be numeric, was [invalid]");
    }

    @Test
    public void testShouldNotAllowTheValueIsNotBetweenTheRangeForMessageRetentionPeriod() {
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("59");

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "messageRetentionPeriod parameter must be between [60, 1209600], was [59]");
    }

    @Test
    public void testShouldNotAllowAlphabeticValueForMessageVisibilityTimeout() {
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("60");
        when(sqsConfig.getMessageVisibilityTimeout()).thenReturn("invalid");

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "messageVisibilityTimeout parameter must be numeric, was [invalid]");
    }

    @Test
    public void testShouldNotAllowTheValueIsNotBetweenTheRangeForMessageVisibilityTimeout() {
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("60");
        when(sqsConfig.getMessageVisibilityTimeout()).thenReturn("1000000");

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "messageRetentionPeriod parameter must be between [0, 43200], was [1000000]");
    }

    @Test
    public void testShouldNotAllowEmptyQueueName() {
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("60");
        when(sqsConfig.getMessageVisibilityTimeout()).thenReturn("0");

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "queueName parameter must not be empty");
    }

    @Test
    public void testShouldNotAllowEmptyRegion() {
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("60");
        when(sqsConfig.getMessageVisibilityTimeout()).thenReturn("0");
        when(sqsConfig.getQueueName()).thenReturn(randomAlphabetic(10));

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "region parameter must not be empty");
    }

    @Test
    public void testShouldNotAllowInvalidRegion() {
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("60");
        when(sqsConfig.getMessageVisibilityTimeout()).thenReturn("0");
        when(sqsConfig.getQueueName()).thenReturn(randomAlphabetic(10));
        when(sqsConfig.getRegion()).thenReturn("invalid-aws-region");

        assertThatThrownBy(() -> sqsQueueHelper.init()).isInstanceOf(IllegalArgumentException.class).hasMessage(
            "[invalid-aws-region] is not a valid region");
    }

    @Test
    public void testShouldNotCreateANewQueueIfExists() {
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("60");
        when(sqsConfig.getMessageVisibilityTimeout()).thenReturn("0");
        when(sqsConfig.getQueueName()).thenReturn(randomAlphabetic(10));
        when(sqsConfig.getRegion()).thenReturn(Regions.EU_CENTRAL_1.getName());

        sqsQueueHelper.init();

        verify(amazonSQS, never()).createQueue(any(CreateQueueRequest.class));
    }

    @Test
    public void testShouldFailIfQueueCreationAttemptsFails() {
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("60");
        when(sqsConfig.getMessageVisibilityTimeout()).thenReturn("0");

        final String queueName = randomAlphabetic(10);
        when(sqsConfig.getQueueName()).thenReturn(queueName);
        when(sqsConfig.getRegion()).thenReturn(Regions.EU_CENTRAL_1.getName());
        when(amazonSQS.getQueueUrl(anyString())).thenThrow(new QueueDoesNotExistException("expected"));

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
        when(sqsConfig.isCreateQueueIfNotExists()).thenReturn(true);
        when(sqsConfig.getMessageRetentionPeriod()).thenReturn("60");
        when(sqsConfig.getMessageVisibilityTimeout()).thenReturn("0");

        final String queueName = randomAlphabetic(10);
        when(sqsConfig.getQueueName()).thenReturn(queueName);
        when(sqsConfig.getRegion()).thenReturn(Regions.EU_CENTRAL_1.getName());
        when(amazonSQS.getQueueUrl(anyString())).thenThrow(new QueueDoesNotExistException("expected"));

        final SdkHttpMetadata responseMetadata = mock(SdkHttpMetadata.class);
        when(responseMetadata.getHttpStatusCode()).thenReturn(200);

        final CreateQueueResult createQueueResult = new CreateQueueResult();
        createQueueResult.setSdkHttpMetadata(responseMetadata);

        when(amazonSQS.createQueue(any(CreateQueueRequest.class))).thenReturn(createQueueResult);

        sqsQueueHelper.init();

        final ArgumentCaptor<CreateQueueRequest> createQueueRequestArgumentCaptor = ArgumentCaptor.forClass(
                CreateQueueRequest.class);
        verify(amazonSQS).createQueue(createQueueRequestArgumentCaptor.capture());
        assertThat(createQueueRequestArgumentCaptor.getValue().getQueueName()).isEqualTo(queueName);
        assertThat(createQueueRequestArgumentCaptor.getValue().getAttributes()).containsEntry(
                                                                                   QueueAttributeName.VisibilityTimeout
                                                                                           .toString(), "0")
                                                                               .containsEntry(
                                                                                   QueueAttributeName
                                                                                           .MessageRetentionPeriod
                                                                                           .toString(), "60");
    }

}
