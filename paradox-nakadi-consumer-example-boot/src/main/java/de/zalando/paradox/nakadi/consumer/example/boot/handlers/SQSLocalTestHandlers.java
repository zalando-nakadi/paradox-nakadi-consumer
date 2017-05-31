package de.zalando.paradox.nakadi.consumer.example.boot.handlers;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Qualifier;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import com.google.common.collect.ImmutableSet;

import de.zalando.paradox.nakadi.consumer.boot.NakadiEventConsumer;
import de.zalando.paradox.nakadi.consumer.boot.NakadiEventConsumers;
import de.zalando.paradox.nakadi.consumer.boot.NakadiHandler;
import de.zalando.paradox.nakadi.consumer.boot.handlers.NakadiRawContentHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawContentHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventHandler;
import de.zalando.paradox.nakadi.consumer.example.boot.domain.OrderReceived;

@Configuration
@Profile({ "local-simple", "local-zk", "local-zk-simple" })
public class SQSLocalTestHandlers {

    private static final String EVENT_NAME = "order.ORDER_RECEIVED";

    @Bean
    public BatchEventsHandler<OrderReceived> sqsPrintBatchEvents() {
        return new PrintBatchEvents();
    }

    @NakadiHandler(eventName = EVENT_NAME, consumerName = "SQSPrintBatchEvents")
    private static class PrintBatchEvents implements BatchEventsHandler<OrderReceived> {
        private static final Logger LOGGER = LoggerFactory.getLogger(PrintBatchEvents.class);

        @Override
        public void onEvent(final EventTypeCursor cursor, final OrderReceived object) {
            LOGGER.info("### cursor {}", cursor);
            LOGGER.info("### event  {}", object);

            if (shouldThrowException()) {
                throw new RuntimeException("PrintBatchEvents test exception");
            }
        }
    }

    @Bean
    public BatchEventsBulkHandler<OrderReceived> sqsPrintBulkBatchEvents() {
        return new PrintBulkBatchEvents();
    }

    @NakadiHandler(eventName = EVENT_NAME, consumerName = "sqs-test-bulk-events-consumer")
    private static class PrintBulkBatchEvents implements BatchEventsBulkHandler<OrderReceived> {
        private static final Logger LOGGER = LoggerFactory.getLogger(PrintBulkBatchEvents.class);

        @Override
        public void onEvent(final EventTypeCursor cursor, final List<OrderReceived> events) {
            LOGGER.info("### cursor {}", cursor);
            LOGGER.info("### events {} / {}", events.size(), events);

            if (shouldThrowException()) {
                throw new RuntimeException("PrintBulkBatchEvents test exception");
            }
        }
    }

    @Bean
    public BatchEventsBulkHandler<OrderReceived> sqsPrintBulkBatchEvents2() {
        return new BatchEventsBulkHandler<OrderReceived>() {
            private final Logger LOGGER = LoggerFactory.getLogger(PrintBulkBatchEvents.class);

            @Override
            @NakadiHandler(
                eventName = EVENT_NAME, consumerName = "sqs-test-bulk-events-consumer", consumerNamePostfix = true
            )
            public void onEvent(final EventTypeCursor cursor, final List<OrderReceived> events) {
                LOGGER.info("### cursor {}", cursor);
                LOGGER.info("### events {} / {}", events.size(), events);

                if (shouldThrowException()) {
                    throw new RuntimeException("BatchEventsBulkHandler test exception");
                }
            }
        };
    }

    @Bean
    public RawContentHandler sqsPrintRawContent() {
        return new RawContentHandler() {
            private final Logger logger = LoggerFactory.getLogger("PrintRawContent");

            @Override
            @NakadiHandler(eventName = EVENT_NAME, consumerName = "sqs-test-raw-content-consumer")
            public void onEvent(final EventTypeCursor cursor, final String content) {
                logger.info("### cursor {}", cursor);
                logger.info("### raw content {} / {}", content.length(), content);

                if (shouldThrowException()) {
                    throw new RuntimeException("RawContentHandler test exception");
                }
            }
        };
    }

    @Component
    @NakadiHandler(eventName = EVENT_NAME, consumerName = "sqs-test-raw-event-consumer")
    @Profile({ "local-simple", "local-zk", "local-zk-simple" })
    public static class PrintRawEvent implements RawEventHandler {
        private static final Logger LOGGER = LoggerFactory.getLogger(PrintRawEvent.class);

        @Override
        public void onEvent(final EventTypeCursor cursor, final String content) {
            LOGGER.info("### cursor {}", cursor);
            LOGGER.info("### raw event {} / {}", content.length(), content);

            if (shouldThrowException()) {
                throw new RuntimeException("PrintRawEvent test exception");
            }
        }
    }

    @Component
    @NakadiHandler(eventName = EVENT_NAME, consumerName = "sqs-test-json-event-consumer")
    @Profile({ "local-simple", "local-zk", "local-zk-simple" })
    public static class PrintJsonEvent implements JsonEventHandler {
        private static final Logger LOGGER = LoggerFactory.getLogger("JsonEventHandler");

        @Override
        public void onEvent(final EventTypeCursor cursor, final JsonNode jsonNode) {
            LOGGER.info("### cursor {}", cursor);
            LOGGER.info("### json event {} ", jsonNode);

            if (shouldThrowException()) {
                throw new RuntimeException("PrintJsonEvent test exception");
            }
        }
    }

    @Bean
    public JsonEventBulkHandler sqsPrintJsonEventBulk() {
        final Logger logger = LoggerFactory.getLogger("JsonEventBulkHandler");

        return new JsonEventBulkHandler() {
            @Override
            @NakadiHandler(
                eventName = EVENT_NAME, consumerName = "sqs-test-bulk-json-events-consumer", consumerNamePostfix = true
            )
            public void onEvent(final EventTypeCursor cursor, final List<JsonNode> jsonNodes) {
                logger.info("### cursor {}", cursor);
                logger.info("### json events {} / {}", jsonNodes.size(), jsonNodes);

                if (shouldThrowException()) {
                    throw new RuntimeException("JsonEventBulkHandler test exception");
                }
            }
        };
    }

    @Bean
    public NakadiEventConsumers testSQSNakadiEventConsumers() {
        return new NakadiEventConsumers(ImmutableSet.of(NakadiEventConsumer.of(EVENT_NAME, "sqs-test-multi1"),
                    NakadiEventConsumer.of(EVENT_NAME, "sqs-test-multi2")));
    }

    @Bean
    public RawContentHandler sqsPrintMultiRawContent(
            @Qualifier("testSQSNakadiEventConsumers") final NakadiEventConsumers testNakadiEventConsumers) {
        return new NakadiRawContentHandler() {
            private final Logger logger = LoggerFactory.getLogger("MultiRawContentHandler");

            @Override
            public NakadiEventConsumers getNakadiEventConsumers() {
                return testNakadiEventConsumers;
            }

            @Override
            public void onEvent(final EventTypeCursor cursor, final String content) {
                logger.info("### cursor {}", cursor);
                logger.info("### raw content {} / {}", content.length(), content);

                if (shouldThrowException()) {
                    throw new RuntimeException("RawContentHandler test exception");
                }
            }
        };
    }

    private static boolean shouldThrowException() {
        return RandomUtils.nextInt(0, 1000000) > 900000;
    }
}
