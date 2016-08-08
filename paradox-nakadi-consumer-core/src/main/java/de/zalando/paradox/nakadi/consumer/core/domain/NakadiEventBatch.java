package de.zalando.paradox.nakadi.consumer.core.domain;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NakadiEventBatch<T> {

    private final NakadiCursor cursor;

    private final List<T> events;

    public NakadiEventBatch(@JsonProperty("cursor") final NakadiCursor cursor,
            @JsonProperty("events") final List<T> events) {
        this.cursor = cursor;
        this.events = events;
    }

    public NakadiCursor getCursor() {
        return cursor;
    }

    public List<T> getEvents() {
        return events;
    }

}
