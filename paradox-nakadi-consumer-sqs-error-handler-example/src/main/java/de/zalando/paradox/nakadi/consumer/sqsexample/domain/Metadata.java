package de.zalando.paradox.nakadi.consumer.sqsexample.domain;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.MoreObjects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Metadata {

    @JsonProperty("eid")
    private String eid;

    @JsonProperty("occurred_at")
    private Date occurredAt;

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("eid", eid).add("occurredAt", occurredAt).toString();
    }
}
