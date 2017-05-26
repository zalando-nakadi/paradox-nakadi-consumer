package de.zalando.paradox.nakadi.consumer.boot;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Component;

import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;
import de.zalando.paradox.nakadi.consumer.boot.components.EventTypeConsumer;
import de.zalando.paradox.nakadi.consumer.boot.components.FailedEventSourceMap;
import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.FailedEventSource;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;

@Component
public class FailedEventReplayer {

    private final EventReceiverRegistry eventReceiverRegistry;

    private final FailedEventSourceMap failedEventSourceMap;

    private final ReplayHandler replayHandler;

    private static final FailedEventSource<FailedEvent> NON_EXIST_FAILED_EVENT_SOURCE =
        new FailedEventSource<FailedEvent>() {

            private static final String EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE =
                "Event source name is not available.";

            @Override
            public Optional<FailedEvent> getFailedEvent() {
                throw new IllegalArgumentException(EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE);
            }

            @Override
            public void commit(final FailedEvent failedEvent) {
                throw new IllegalArgumentException(EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE);
            }

            @Override
            public String getEventSourceName() {
                throw new IllegalArgumentException(EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE);
            }

            @Override
            public long getApproximatelyTotalNumberOfFailedEvents() {
                throw new IllegalArgumentException(EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE);
            }
        };

    @Autowired
    public FailedEventReplayer(final EventReceiverRegistry eventReceiverRegistry,
            final FailedEventSourceMap failedEventSourceMap, final ReplayHandler replayHandler) {
        this.eventReceiverRegistry = eventReceiverRegistry;
        this.failedEventSourceMap = failedEventSourceMap;
        this.replayHandler = replayHandler;
    }

    public Long getApproximatelyTotalNumberOfFailedEvents(final String eventSourceName) {
        return failedEventSourceMap.getFailedEventSourceMap()
                                   .getOrDefault(eventSourceName, NON_EXIST_FAILED_EVENT_SOURCE)
                                   .getApproximatelyTotalNumberOfFailedEvents();
    }

    public Collection<String> getFailedEventSources() {
        return failedEventSourceMap.getFailedEventSourceMap().keySet();
    }

    public void replay(final String eventSourceName, final Long numberOfFailedEvents,
            final boolean breakProcessingOnException) {
        final FailedEventSource<FailedEvent> failedEventSource = failedEventSourceMap.getFailedEventSourceMap()
                                                                                     .getOrDefault(eventSourceName,
                                                                                         NON_EXIST_FAILED_EVENT_SOURCE);
        final long approximatelyTotalNumberOfFailedEvents =
            failedEventSource.getApproximatelyTotalNumberOfFailedEvents();

        final long upperBound = numberOfFailedEvents > approximatelyTotalNumberOfFailedEvents
            ? approximatelyTotalNumberOfFailedEvents : numberOfFailedEvents;

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
