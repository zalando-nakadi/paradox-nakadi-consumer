package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.io.IOException;

import java.util.Optional;

import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventCursor;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

abstract class AbstractResponseHandler implements ResponseHandler {

    private static final String BATCH_SEPARATOR = "\n";
    protected final Logger log;
    protected final ObjectMapper jsonMapper;
    protected final EventTypePartition eventTypePartition;
    protected final PartitionCoordinator coordinator;

    AbstractResponseHandler(final EventTypePartition eventTypePartition, final PartitionCoordinator coordinator,
            final Logger log, final ObjectMapper jsonMapper) {
        this.eventTypePartition = eventTypePartition;
        this.coordinator = coordinator;
        this.log = log;
        this.jsonMapper = jsonMapper;
    }

    String[] getEvents(final String string) {
        return string.split(BATCH_SEPARATOR);
    }

    Optional<NakadiEventCursor> getEventCursor(final String string) {
        try {
            return Optional.of(jsonMapper.readValue(string, NakadiEventCursor.class));
        } catch (IOException e) {
            log.error("Error while parsing event cursor from [{}]", string, e);
            return Optional.empty();
        }
    }
}
