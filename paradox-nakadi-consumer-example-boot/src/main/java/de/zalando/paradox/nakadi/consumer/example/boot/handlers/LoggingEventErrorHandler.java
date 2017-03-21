package de.zalando.paradox.nakadi.consumer.example.boot.handlers;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Service;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;

@Service
public class LoggingEventErrorHandler implements EventErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingEventErrorHandler.class);

    @Override
    public void onError(final Throwable t, final EventTypePartition eventTypePartition, @Nullable final String offset,
            final String rawEvent) {
        LOGGER.error(
            "Failed Event // Event Partition = [{}] , Event Type = [{}] , Event Offset = [{}] , Raw Event = [{}] //",
            eventTypePartition.getPartition(), eventTypePartition.getEventType(), offset, rawEvent, t);
    }
}
