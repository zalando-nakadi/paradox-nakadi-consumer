package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

@FunctionalInterface
public interface ResponseHandlerFactory {
    ResponseHandler get(final EventTypePartition eventTypePartition, final ObjectMapper jsonMapper);
}
