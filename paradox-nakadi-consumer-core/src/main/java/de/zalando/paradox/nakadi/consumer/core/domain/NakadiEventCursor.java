package de.zalando.paradox.nakadi.consumer.core.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NakadiEventCursor {

    private final NakadiCursor cursor;

    public NakadiEventCursor(@JsonProperty("cursor") final NakadiCursor cursor) {
        this.cursor = cursor;
    }

    public NakadiCursor getCursor() {
        return cursor;
    }

}
