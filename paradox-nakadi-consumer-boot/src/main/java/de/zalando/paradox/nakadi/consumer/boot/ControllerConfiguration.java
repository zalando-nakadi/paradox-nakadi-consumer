package de.zalando.paradox.nakadi.consumer.boot;

import static java.util.Objects.requireNonNull;

import static org.springframework.http.ResponseEntity.badRequest;
import static org.springframework.http.ResponseEntity.ok;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.context.annotation.Configuration;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;
import de.zalando.paradox.nakadi.consumer.boot.components.EventTypeConsumer;
import de.zalando.paradox.nakadi.consumer.boot.components.FailedEventSourceNameResponse;
import de.zalando.paradox.nakadi.consumer.boot.components.NumberOfFailedEventsResponse;
import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.client.Client;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;

import rx.Single;

@Configuration
public class ControllerConfiguration {

    @RestController
    @RequestMapping(value = "/nakadi/failed-event-sources")
    public static class FailedEventReplayController {

        private final FailedEventReplayer failedEventReplayer;

        @Autowired
        public FailedEventReplayController(final FailedEventReplayer failedEventReplayer) {
            this.failedEventReplayer = failedEventReplayer;
        }

        @RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
        public FailedEventSourceNameResponse getFailedEventSources() {
            return new FailedEventSourceNameResponse(failedEventReplayer.getFailedEventSources());
        }

        @RequestMapping(value = "/{event_source_name:.+}", produces = MediaType.APPLICATION_JSON_VALUE)
        public NumberOfFailedEventsResponse getApproximatelyTotalNumberOfFailedEvents(
                @PathVariable("event_source_name") final String eventSourceName) {
            return new NumberOfFailedEventsResponse(failedEventReplayer.getApproximatelyTotalNumberOfFailedEvents(
                        eventSourceName));
        }

        @RequestMapping(
            value = "/{event_source_name:.+}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE
        )
        public ResponseEntity<?> replay(@PathVariable("event_source_name") final String eventSourceName,
                @RequestParam("number_of_failed_events") final long numberOfFailedEvents,
                @RequestParam(value = "break_processing_on_exception", defaultValue = "false") final boolean breakProcessingOnException) {

            try {
                failedEventReplayer.replay(eventSourceName, numberOfFailedEvents, breakProcessingOnException);
                return ResponseEntity.ok().build();
            } catch (Exception ex) {
                return ResponseEntity.badRequest().body(ex);
            }
        }

        public DeferredResult<ResponseEntity<?>> replay(final String eventSourceName, final String eventId) {
            throw new UnsupportedOperationException();
        }

        public DeferredResult<ResponseEntity<?>> replay(final String eventSourceName, final String eventType,
                final String eventId) {
            throw new UnsupportedOperationException();
        }

        public DeferredResult<ResponseEntity<?>> replay(final String eventSourceName, final FailedEvent failedEvent) {
            throw new UnsupportedOperationException();
        }

        public DeferredResult<ResponseEntity<?>> replay(final String eventSourceName,
                final List<FailedEvent> failedEvent) {
            throw new UnsupportedOperationException();
        }

        public DeferredResult<ResponseEntity<?>> discard(final String eventSourceName, final FailedEvent failedEvent) {
            throw new UnsupportedOperationException();
        }

        public DeferredResult<ResponseEntity<?>> discard(final String eventSourceName, final String eventId) {
            throw new UnsupportedOperationException();
        }
    }

    @RestController
    @RequestMapping(value = "/nakadi/event-receivers")
    public static class EventReceiverController {

        private EventReceiverRegistry registry;

        @Autowired
        public EventReceiverController(final EventReceiverRegistry registry) {
            this.registry = registry;
        }

        @RequestMapping(value = "/stop", method = RequestMethod.POST)
        public void stop() throws Exception {
            registry.stop();
        }

        @RequestMapping(value = "/restart", method = RequestMethod.POST)
        public void restart() {
            registry.restart();
        }
    }

    @RestController
    @RequestMapping(value = "/nakadi/event-handlers")
    public static class EventHandlerController {
        private static final long DEFERRED_TIMEOUT = 20000L;
        private final ReplayHandler replayHandler = new ReplayHandler();

        private EventReceiverRegistry registry;

        private Client client;

        @Autowired
        public EventHandlerController(final EventReceiverRegistry registry, final Client client) {
            this.registry = registry;
            this.client = client;
        }

        @RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
        public Set<EventTypeConsumer> getAllEventTypeConsumers() throws Exception {
            return registry.getEventTypeConsumers();
        }

        @RequestMapping(
            value = "/event_types/{event_type:.+}/partitions/{partition}/offsets/{offset}/replays",
            produces = MediaType.TEXT_PLAIN_VALUE, method = RequestMethod.POST
        )
        public DeferredResult<ResponseEntity<?>> replay(@PathVariable(value = "event_type") final String eventName,
                @PathVariable(value = "partition") final String partition,
                @PathVariable(value = "offset") final String offset,
                @RequestParam(value = "consumer_name", required = false) final String consumerName,
                @RequestParam(value = "verbose", required = false, defaultValue = "false") final boolean verbose) {

            if (validateConsumerNameAndEventType(consumerName, eventName)) {
                return getBadDeferredResult();
            }

            final EventTypePartition eventTypePartition = EventTypePartition.of(EventType.of(eventName), partition);
            final EventTypeCursor queryCursor = replayHandler.getQueryCursor(EventTypeCursor.of(eventTypePartition,
                        offset));
            final Single<String> singleContent = client.getContent(queryCursor);
            return deferredReplayResult(eventTypePartition, consumerName, verbose, singleContent);
        }

        @RequestMapping(
            value = "/event_types/{event_type:.+}/partitions/{partition}/restores",
            produces = MediaType.TEXT_PLAIN_VALUE, method = RequestMethod.POST
        )
        public DeferredResult<ResponseEntity<?>> restore(@PathVariable(value = "event_type") final String eventName,
                @PathVariable(value = "partition") final String partition,
                @RequestParam(value = "consumer_name", required = false) final String consumerName,
                @RequestParam(value = "verbose", required = false, defaultValue = "false") final boolean verbose,
                @RequestBody final String content) {

            if (validateConsumerNameAndEventType(consumerName, eventName)) {
                return getBadDeferredResult();
            }

            final EventTypePartition eventTypePartition = EventTypePartition.of(EventType.of(eventName), partition);
            final Single<String> singleContent = Single.just(content);
            return deferredReplayResult(eventTypePartition, consumerName, verbose, singleContent);
        }

        private DeferredResult<ResponseEntity<?>> deferredReplayResult(final EventTypePartition eventTypePartition,
                final String consumerName, final boolean verbose, final Single<String> singleContent) {

            final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<>(DEFERRED_TIMEOUT);
            final Set<EventTypeConsumer> consumers = registry.getEventTypeConsumers().stream()
                                                             .filter(replayHandler.filterConsumer(
                                                                     eventTypePartition.getName(), consumerName))
                                                             .collect(Collectors.toSet());
            if (consumers.isEmpty()) {
                deferredResult.setErrorResult(ResponseEntity.notFound().build());
            } else {
                singleContent.subscribe(content -> {
                        consumers.forEach(eventTypeConsumer -> {
                            final EventHandler<?> handler = requireNonNull(
                                    registry.getEventTypeConsumerHandler(eventTypeConsumer), "handler not found");
                            replayHandler.handle(consumerName, handler, eventTypePartition, content);
                        });
                        deferredResult.setResult(ok(verbose ? content : ""));

                    },
                    throwable -> deferredResult.setErrorResult(badRequest().body(throwable.toString())));
            }

            return deferredResult;
        }

        private DeferredResult<ResponseEntity<?>> getBadDeferredResult() {
            final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<>();
            deferredResult.setErrorResult(badRequest().body("Consumer not found."));
            return deferredResult;
        }

        private boolean validateConsumerNameAndEventType(final String consumerName, final String eventType) {
            return registry.getEventTypeConsumers().stream().noneMatch(replayHandler.filterConsumer(eventType,
                        consumerName));
        }
    }
}
