package de.zalando.paradox.nakadi.consumer.core.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.MoreObjects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NakadiCursor {

    private final String partition;

    private final String offset;

    public NakadiCursor(@JsonProperty("partition") final String partition,
            @JsonProperty("offset") final String offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public String getPartition() {
        return partition;
    }

    public String getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("partition", partition).add("offset", offset).toString();
    }
}
