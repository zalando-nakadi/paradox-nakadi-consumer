package de.zalando.paradox.nakadi.consumer.core.http.handlers.testdomain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.MoreObjects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderReceived {

    @JsonProperty("metadata")
    private Metadata metadata;

    @JsonProperty("order_number")
    private String orderNumber;

    public String getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(final String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(final Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("metadata", metadata).add("orderNumber", orderNumber).toString();
    }
}
