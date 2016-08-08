package de.zalando.paradox.nakadi.consumer.core.http.handlers.testdomain;

import java.util.Date;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.MoreObjects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Metadata {

    @JsonProperty("eid")
    private UUID eid;

    @JsonProperty("occurred_at")
    private Date occurredAt;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("received_at")
    private Date receivedAt;

    @JsonProperty("flow_id")
    private String flowId;

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("eid", eid).add("occurredAt", occurredAt)
                          .add("eventType", eventType).add("receivedAt", receivedAt).add("flowId", flowId).toString();
    }
}
