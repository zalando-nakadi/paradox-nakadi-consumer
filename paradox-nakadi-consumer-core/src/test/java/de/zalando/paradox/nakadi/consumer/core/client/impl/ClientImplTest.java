package de.zalando.paradox.nakadi.consumer.core.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Ignore;
import org.junit.Test;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.testdomain.OrderReceived;

import rx.Single;

public class ClientImplTest {

    private static final String NAKADI_URL = "http://localhost:8080";

    private static EventType ORDER_RECEIVED = EventType.of("order.ORDER_RECEIVED");

    private static ClientImpl clientImpl = ClientImpl.Builder.of(NAKADI_URL).build();

    @Test
    public void testShouldThrowWhenRequestingUnconsumedEventsWithNullCursors() {
        assertThatThrownBy(() -> { clientImpl.getCursorsLag(null); }).isInstanceOf(NullPointerException.class)
                                  .hasMessage("cursors must not be null");
    }

    @Test
    public void testShouldThrowWhenRequestingUnconsumedEventsWithEmptyCursors() {
        assertThatThrownBy(() -> { clientImpl.getCursorsLag(Collections.emptyList()); }).isInstanceOf(
            IllegalArgumentException.class).hasMessage("cursors must not be empty");
    }

    @Test
    public void testShouldThrowWhenRequestingUnconsumedEventsForDifferentEventTypes() {
        final EventTypeCursor eventTypeCursor = EventTypeCursor.of(EventTypePartition.of(EventType.of("test-event"),
                    "0"), "BEGIN");
        final EventTypeCursor eventTypeCursorDifferentType = EventTypeCursor.of(EventTypePartition.of(
                    EventType.of("test-event-two"), "0"), "BEGIN");
        assertThatThrownBy(() -> {
                                      clientImpl.getCursorsLag(
                                          Arrays.asList(eventTypeCursor, eventTypeCursorDifferentType));
                                  }).isInstanceOf(IllegalArgumentException.class).hasMessage(
                                      "cursors must contain cursors of only one type");
    }

    @Test
    @Ignore("Needs Nakadi server")
    public void testGetNakadiPartitionsFound() {
        final List<NakadiPartition> list = clientImpl.getPartitions(ORDER_RECEIVED).toBlocking().value();
        assertThat(list).isNotEmpty();
    }

    @Test
    @Ignore("Needs Nakadi server")
    public void testGetNakadiPartitionsNotFound() {
        final List<NakadiPartition> list = clientImpl.getPartitions(EventType.of("__not_configured__"))
                                                     .onErrorReturn(throwable -> Collections.emptyList()).toBlocking()
                                                     .value();
        assertThat(list).isEmpty();
    }

    @Test
    @Ignore("Needs Nakadi server")
    public void testGetNakadiEventPayload() {
        final EventTypeCursor cursor = EventTypeCursor.of(EventTypePartition.of(ORDER_RECEIVED, "0"), "0");
        final String event = clientImpl.getEvent(cursor).toBlocking().value();
        assertThat(event).startsWith("{\"metadata\":{");
        assertThat(event).contains("\"order_number\"");
    }

    @Test
    @Ignore("Needs Nakadi server")
    public void testGetNakadiEventStream() {
        final EventTypeCursor cursor = EventTypeCursor.of(EventTypePartition.of(ORDER_RECEIVED, "0"), "BEGIN");
        final String stream = clientImpl.getContent(cursor).toBlocking().value();
        assertThat(stream).startsWith("{\"cursor\":{\"partition\":\"0\",\"offset\":\"0\"}");
        assertThat(stream).contains("\"order_number\"");
    }

    @Test
    @Ignore("Needs Nakadi server")
    public void testGetNakadiEventObject() {
        final EventTypeCursor cursor = EventTypeCursor.of(EventTypePartition.of(ORDER_RECEIVED, "0"), "0");
        final OrderReceived event = getEvent(cursor, OrderReceived.class).toBlocking().value();
        assertThat(event.getOrderNumber()).isNotEmpty();
    }

    @Test
    @Ignore("Needs Nakadi server")
    public void testGetNakadiEventNotFound() {
        final EventTypeCursor cursor = EventTypeCursor.of(EventTypePartition.of(ORDER_RECEIVED, "0"), "100000");
        final String event = clientImpl.getEvent(cursor).onErrorReturn(throwable -> null).toBlocking().value();
        assertThat(event).isNull();
    }

    private <T> Single<T> getEvent(final EventTypeCursor cursor, final Class<T> clazz) {
        return clientImpl.getEvent(cursor).map(content -> getEvent0(content, clazz)).map(o -> o.orElse(null));
    }

    private <T> Optional<T> getEvent0(final String content, final Class<T> clazz) {
        try {
            final T event = clientImpl.getObjectMapper().readValue(content, clazz);
            return Optional.of(event);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
