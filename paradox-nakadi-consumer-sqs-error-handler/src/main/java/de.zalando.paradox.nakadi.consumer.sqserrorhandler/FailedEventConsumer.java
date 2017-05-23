package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import com.amazonaws.services.sqs.AmazonSQS;

public class FailedEventConsumer {

    private AmazonSQS amazonSQS;

    private SQSConfiguration sqsConfiguration;

    public void handle() { }
}
