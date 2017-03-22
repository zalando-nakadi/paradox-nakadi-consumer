package de.zalando.paradox.nakadi.consumer.boot;

public class NakadiConsumerSettings {

    private Integer eventsBatchTimeoutSeconds;

    private Integer eventsBatchLimit;

    public Integer getEventsBatchTimeoutSeconds() {
        return eventsBatchTimeoutSeconds;
    }

    public void setEventsBatchTimeoutSeconds(final Integer eventsBatchTimeoutSeconds) {
        this.eventsBatchTimeoutSeconds = eventsBatchTimeoutSeconds;
    }

    public Integer getEventsBatchLimit() {
        return eventsBatchLimit;
    }

    public void setEventsBatchLimit(final Integer eventsBatchLimit) {
        this.eventsBatchLimit = eventsBatchLimit;
    }

}
