package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.paradox.nakadi.consumer.core.ConsumerConfig;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.http.HttpReactiveReceiver;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawContentHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.requests.HttpGetPartitionsHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class ZKLeaderHttpEventReceiverTestConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKLeaderHttpEventReceiverTestConsumer.class);

    public static void main(final String[] args) throws Exception {
        final String baseUri = "http://localhost:8080";
        final String eventName = "order.ORDER_RECEIVED";
        final String consumerName = "zk-test-consumer";

        final ZKHolder zkHolder = new ZKHolder("localhost:2181", null, null);
        zkHolder.init();

        final PartitionCoordinator coordinator = new ZKLeaderConsumerPartitionCoordinator(zkHolder, consumerName,
                Collections.emptyList());
        coordinator.init();

        try {
            final ConsumerConfig config = getBatchEventsConfig(baseUri, eventName, coordinator);
            final HttpReactiveReceiver receiver = new HttpReactiveReceiver(new HttpGetPartitionsHandler(config));
            receiver.init();
            LOGGER.info("Receiver started .....");

            Thread.sleep(Long.MAX_VALUE);

        } finally {
            LOGGER.info("Close receiver");
            coordinator.close();
            Thread.sleep(2000);
        }

        LOGGER.info("Finished");
    }

    @SuppressWarnings("unused")
    private static ConsumerConfig getRawEventConfig(final String baseUri, final String eventName,
            final PartitionCoordinator coordinator) {
        final RawEventHandler handler = (cursor, content) -> {
            LOGGER.info("### cursor  {}", cursor);
            LOGGER.info("### raw event  {}", content);
        };

        return new ConsumerConfig.Builder(baseUri, eventName, coordinator).withRawEventHandler(handler).build();
    }

    @SuppressWarnings("unused")
    private static ConsumerConfig getRawContentConfig(final String baseUri, final String eventName,
            final PartitionCoordinator coordinator) {
        final RawContentHandler handler = (cursor, content) -> {
            LOGGER.info("### cursor  {}", cursor);
            LOGGER.info("### raw content  {}", content);
        };
        return new ConsumerConfig.Builder(baseUri, eventName, coordinator).withRawContentHandler(handler).build();
    }

    @SuppressWarnings("unused")
    private static ConsumerConfig getBatchEventsConfig(final String baseUri, final String eventName,
            final PartitionCoordinator coordinator) {

        final BatchEventsHandler<Object> handler = new BatchEventsHandler<Object>() {
            @Override
            public void onEvent(final EventTypeCursor cursor, final Object object) {
                LOGGER.info("### cursor  {}", cursor);
                LOGGER.info("### event   {}", object);
            }

            @Override
            public Class<Object> getEventClass() {
                return Object.class;
            }
        };

        return ConsumerConfig.Builder.of(baseUri, eventName, coordinator).withBatchEventsHandler(handler).build();
    }
}
