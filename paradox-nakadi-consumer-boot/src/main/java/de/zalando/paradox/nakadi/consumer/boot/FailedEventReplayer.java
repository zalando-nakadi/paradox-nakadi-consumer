package de.zalando.paradox.nakadi.consumer.boot;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;
import de.zalando.paradox.nakadi.consumer.boot.components.EventTypeConsumer;
import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.FailedEventSource;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;

public class FailedEventReplayer {

    private final EventReceiverRegistry eventReceiverRegistry;

    private final Map<String, FailedEventSource> failedEventSourceMap;

    private final ReplayHandler replayHandler;

    private static final String EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE = "Event source name is not available.";

    private static final Logger LOGGER = LoggerFactory.getLogger(FailedEventReplayer.class);

    public FailedEventReplayer(final EventReceiverRegistry eventReceiverRegistry,
            final List<FailedEventSource> failedEventSources, final ReplayHandler replayHandler) {
        this.eventReceiverRegistry = eventReceiverRegistry;
        this.replayHandler = replayHandler;

        failedEventSourceMap = failedEventSources.stream().collect(Collectors.toMap(
                    FailedEventSource::getEventSourceName, Function.identity()));
    }

    public Long getTotalNumberOfFailedEvents(final String eventSourceName) {
        checkArgument(failedEventSourceMap.containsKey(eventSourceName), EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE);
        return failedEventSourceMap.get(eventSourceName).getSize();
    }

    public Collection<String> getFailedEventSources() {
        return failedEventSourceMap.keySet();
    }

    public void replay(final String eventSourceName, final Long numberOfFailedEvents,
            final boolean breakProcessingOnException) {
        checkArgument(failedEventSourceMap.containsKey(eventSourceName), EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE);

        final FailedEventSource<FailedEvent> failedEventSource = failedEventSourceMap.get(eventSourceName);
        final long totalNumberOfFailedEvents = failedEventSource.getSize();

        final long upperBound = Math.min(numberOfFailedEvents, totalNumberOfFailedEvents);

        Optional<FailedEvent> failedEventOptional = Optional.empty();
        for (long counter = 0; counter < upperBound; counter++) {

            try {
                failedEventOptional = failedEventSource.getFailedEvent();
                failedEventOptional.ifPresent(failedEvent -> replay(failedEvent, failedEventSource));
            } catch (final Exception ex) {
                if (breakProcessingOnException) {
                    throw new IllegalStateException(String.format(
                            "Exception occurred while processing the event. Event = [%s]",
                            failedEventOptional.orElse(null)), ex);
                } else {
                    LOGGER.error(String.format("Exception occurred while processing the event source = [%s]",
                            eventSourceName), ex);
                }
            }
        }
    }

    private void replay(final FailedEvent failedEvent, final FailedEventSource<FailedEvent> failedEventSource) {
        final EventHandler<?> handler = requireNonNull(eventReceiverRegistry.getEventTypeConsumerHandler(
                    new EventTypeConsumer(failedEvent.getEventType().getName(), failedEvent.getConsumerName())),
                "handler not found");
        replayHandler.handle(failedEvent.getConsumerName(), handler,
            new EventTypePartition(failedEvent.getEventType(), failedEvent.getPartition()), failedEvent.getRawEvent());

        failedEventSource.commit(failedEvent);
    }
}
