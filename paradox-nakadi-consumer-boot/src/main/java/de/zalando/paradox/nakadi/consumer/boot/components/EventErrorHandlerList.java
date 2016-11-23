package de.zalando.paradox.nakadi.consumer.boot.components;

import java.util.Collections;
import java.util.List;

import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;

public class EventErrorHandlerList {

    private final List<EventErrorHandler> eventErrorHandlerList;

    public EventErrorHandlerList() {
        this(Collections.emptyList());
    }

    public EventErrorHandlerList(final List<EventErrorHandler> eventErrorHandlers) {
        this.eventErrorHandlerList = eventErrorHandlers;
    }

    public List<EventErrorHandler> getEventErrorHandlerList() {
        return eventErrorHandlerList;
    }
}
