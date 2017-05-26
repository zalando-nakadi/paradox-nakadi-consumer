package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;

public class SQSFailedEvent extends FailedEvent {

    public SQSFailedEvent(final FailedEvent failedEvent) {
        super(failedEvent);
    }

    private String receiptHandle;

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public void setReceiptHandle(final String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }
}
