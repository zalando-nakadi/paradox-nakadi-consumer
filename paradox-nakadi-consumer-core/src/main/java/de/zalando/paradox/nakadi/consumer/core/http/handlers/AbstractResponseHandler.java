package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.io.IOException;

import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventCursor;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

abstract class AbstractResponseHandler implements ResponseHandler {

    private static final String BATCH_SEPARATOR = "\n";
    protected final Logger log;
    protected final ObjectMapper jsonMapper;
    protected final EventTypePartition eventTypePartition;
    protected final PartitionCoordinator coordinator;
    protected final String consumerName;

    AbstractResponseHandler(final String consumerName, final EventTypePartition eventTypePartition,
            final PartitionCoordinator coordinator, final Logger log, final ObjectMapper jsonMapper) {
        this.eventTypePartition = eventTypePartition;
        this.coordinator = coordinator;
        this.log = log;
        this.jsonMapper = jsonMapper;
        this.consumerName = consumerName;
    }

    String[] getEvents(final String string) {
        return string.split(BATCH_SEPARATOR);
    }

    NakadiEventCursor getEventCursor(final String string) {
        try {
            return jsonMapper.readValue(string, NakadiEventCursor.class);
        } catch (IOException e) {
            log.error("Error while parsing event cursor from [{}]", string, e);
            ThrowableUtils.throwException(e);
            return null;
        }
    }
}
