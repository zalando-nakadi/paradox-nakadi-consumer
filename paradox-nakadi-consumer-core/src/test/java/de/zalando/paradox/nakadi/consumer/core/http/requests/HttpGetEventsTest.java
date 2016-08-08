package de.zalando.paradox.nakadi.consumer.core.http.requests;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import de.zalando.paradox.nakadi.consumer.core.EventStreamConfig;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

public class HttpGetEventsTest {

    private static final EventTypeCursor CURSOR = EventTypeCursor.of(EventTypePartition.of(
                EventType.of("order.CREATED.1"), "12"), "1234");

    @Test
    public void testURLWithOutParameters() {
        //J-
        final HttpGetEvents httpGetEvents = new HttpGetEvents("http://localhost:8080", CURSOR, null);
        assertThat(httpGetEvents.getUrl().toString()).
                isEqualTo("http://localhost:8080/event-types/order.CREATED.1/events");
        //J+
    }

    @Test
    public void testURLWithStreamTimeout() {
        //J-
        final EventStreamConfig eventStreamConfig = EventStreamConfig.Builder.of().
                withStreamTimeoutSeconds(900).build();
        final HttpGetEvents httpGetEvents = new HttpGetEvents("http://localhost:8080", CURSOR, eventStreamConfig);
        assertThat(httpGetEvents.getUrl().toString()).
                isEqualTo("http://localhost:8080/event-types/order.CREATED.1/events?stream_timeout=900");
        //J+
    }

    @Test
    public void testURLWithStreamParameters() {
        //J-
        final EventStreamConfig eventStreamConfig = EventStreamConfig.Builder.of().
                withStreamTimeoutSeconds(60).
                withStreamLimit(1000).
                withBatchLimit(1).
                withBatchTimeoutSeconds(30).
                withStreamKeepAliveLimit(0).

                build();
        final HttpGetEvents httpGetEvents = new HttpGetEvents("http://localhost:8080", CURSOR, eventStreamConfig);
        assertThat(httpGetEvents.getUrl().toString()).
                isEqualTo("http://localhost:8080/event-types/order.CREATED.1/events?" +
                        "stream_timeout=60&" +
                        "stream_keep_alive_limit=0&" +
                        "batch_limit=1&" +
                        "batch_flush_timeout=30&" +
                        "stream_limit=1000");
        //J+
    }
}
