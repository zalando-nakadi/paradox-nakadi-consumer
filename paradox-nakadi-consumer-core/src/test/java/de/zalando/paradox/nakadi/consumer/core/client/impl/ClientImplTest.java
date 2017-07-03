package de.zalando.paradox.nakadi.consumer.core.client.impl;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.exceptions.InvalidEventTypeException;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.testdomain.OrderReceived;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

import rx.Single;

public class ClientImplTest {

    private static final EventType ORDER_RECEIVED = EventType.of("order.ORDER_RECEIVED");

    private static final String FIRST_ORDER = "ORDER_001";
    private static final String SECOND_ORDER = "ORDER_002";

    private static final String WRONG_EVENT_TYPE = "__not_configured__";

    private static final String NAKADI_RESPONSE_PARTITIONS = "[{\"oldest_available_offset\":\"000000000000000000\","
            + "\"newest_available_offset\":\"BEGIN\",\"partition\":\"0\"}]";
    private static final String NAKADI_RESPONSE_ONE_ORDER = "{\"cursor\":{\"partition\":\"0\","
            + "\"offset\":\"000000000000000000\"},\"events\":[{\"metadata\": "
            + "{\"eid\": \"4ae5011e-eb01-11e5-8b4a-1c6f65464fc6\",\"occurred_at\": \"2016-03-15T23:56:11+01:00\"},"
            + "\"order_number\": \"" + FIRST_ORDER + "\"}]}";
    private static final String NAKADI_RESPONSE_NO_EVENTS_FOUND = "{\"cursor\":{\"partition\":\"0\","
            + "\"offset\":\"000000000000000000\"},\"events\":[]}";
    private static final String NAKADI_RESPONSE_NOT_FOUND = "{\"type\":\"http://httpstatus.es/404\","
            + "\"title\":\"Not Found\",\"status\":404,\"detail\":\"topic not found\"}";
    private static final String NAKADI_RESPONSE_TWO_ORDERS_ONE_WRONG_TYPE = "{\"cursor\":{\"partition\":\"0\","
            + "\"offset\":\"000000000000000000\"},\"events\":[{\"metadata\": "
            + "{\"eid\": \"4ae5011e-eb01-11e5-8b4a-1c6f65464fc6\",\"occurred_at\": \"2016-03-15T23:56:11+01:00\","
            + "\"event_type\": \"" + WRONG_EVENT_TYPE + "\"},\"order_number\": \"" + FIRST_ORDER + "\"},{\"metadata\": "
            + "{\"eid\": \"4ae5011e-eb01-11e5-8b4a-1c6f65464fc6\",\"occurred_at\": \"2016-03-15T23:56:11+01:00\","
            + "\"event_type\": \"" + ORDER_RECEIVED.getName() + "\"},\"order_number\": \"" + SECOND_ORDER + "\"}]}";

    private MockWebServer mockNakadi;

    private static ClientImpl clientImpl;

    @Before
    public void setUp() throws IOException {
        mockNakadi = new MockWebServer();
        mockNakadi.start(0);
        clientImpl = ClientImpl.Builder.of(mockNakadi.url("/").toString()).build();
    }

    @After
    public void tearDown() throws IOException {
        mockNakadi.close();
    }

    @Test
    public void testShouldThrowWhenRequestingUnconsumedEventsWithNullCursors() {
        assertThatThrownBy(() -> clientImpl.getCursorsLag(null)).isInstanceOf(NullPointerException.class).hasMessage(
            "cursors must not be null");
    }

    @Test
    public void testShouldThrowWhenRequestingUnconsumedEventsWithEmptyCursors() {
        assertThatThrownBy(() -> clientImpl.getCursorsLag(Collections.emptyList())).isInstanceOf(
            IllegalArgumentException.class).hasMessage("cursors must not be empty");
    }

    @Test
    public void testShouldThrowWhenRequestingUnconsumedEventsForDifferentEventTypes() {
        final EventTypeCursor eventTypeCursor = EventTypeCursor.of(EventTypePartition.of(EventType.of("test-event"),
                    "0"), "BEGIN");
        final EventTypeCursor eventTypeCursorDifferentType = EventTypeCursor.of(EventTypePartition.of(
                    EventType.of("test-event-two"), "0"), "BEGIN");
        assertThatThrownBy(() ->
                                        clientImpl.getCursorsLag(
                                            Arrays.asList(eventTypeCursor, eventTypeCursorDifferentType))).isInstanceOf(
            IllegalArgumentException.class).hasMessage("cursors must contain cursors of only one type");
    }

    @Test
    public void testGetNakadiPartitionsFound() throws IOException {
        mockNakadi.enqueue(new MockResponse().setBody(NAKADI_RESPONSE_PARTITIONS));

        final List<NakadiPartition> list = clientImpl.getPartitions(ORDER_RECEIVED).toBlocking().value();
        assertThat(list).isNotEmpty();
    }

    @Test
    public void testGetNakadiPartitionsNotFound() throws IOException {
        mockNakadi.enqueue(new MockResponse().setResponseCode(404).setBody(NAKADI_RESPONSE_NOT_FOUND));

        final List<NakadiPartition> list = clientImpl.getPartitions(EventType.of(WRONG_EVENT_TYPE))
                                                     .onErrorReturn(throwable -> Collections.emptyList()).toBlocking()
                                                     .value();
        assertThat(list).isEmpty();
    }

    @Test
    public void testGetNakadiEventPayload() throws IOException {
        mockNakadi.enqueue(new MockResponse().setBody(NAKADI_RESPONSE_ONE_ORDER));

        final EventTypeCursor cursor = EventTypeCursor.of(EventTypePartition.of(ORDER_RECEIVED, "0"), "0");
        final String event = clientImpl.getEvent(cursor).toBlocking().value();
        assertThat(event).startsWith("{\"metadata\":{");
        assertThat(event).contains("\"order_number\"");
    }

    @Test
    public void testGetNakadiEventStream() throws IOException {
        mockNakadi.enqueue(new MockResponse().setBody(NAKADI_RESPONSE_ONE_ORDER));

        final EventTypeCursor cursor = EventTypeCursor.of(EventTypePartition.of(ORDER_RECEIVED, "0"), "BEGIN");
        final String stream = clientImpl.getContent(cursor).toBlocking().value();
        assertThat(stream).startsWith("{\"cursor\":{\"partition\":\"0\",\"offset\":\"000000000000000000\"}");
        assertThat(stream).contains("\"order_number\"");
    }

    @Test
    public void testGetNakadiEventObject() throws IOException {
        mockNakadi.enqueue(new MockResponse().setBody(NAKADI_RESPONSE_ONE_ORDER));

        final EventTypeCursor cursor = EventTypeCursor.of(EventTypePartition.of(ORDER_RECEIVED, "0"), "0");
        final OrderReceived event = getEvent(cursor, OrderReceived.class).toBlocking().value();
        assertThat(event.getOrderNumber()).isNotEmpty();
    }

    @Test
    public void testGetNakadiEventNotFound() throws IOException {
        mockNakadi.enqueue(new MockResponse().setBody(NAKADI_RESPONSE_NO_EVENTS_FOUND));

        final EventTypeCursor cursor = EventTypeCursor.of(EventTypePartition.of(ORDER_RECEIVED, "0"),
                "00000000000100000");
        final String event = clientImpl.getEvent(cursor).onErrorReturn(throwable -> null).toBlocking().value();
        assertThat(event).isNull();
    }

    @Test
    public void testShouldStopProcessingAndThrowExceptionWhenItReceivesWrongEventTypes() throws IOException {
        mockNakadi.enqueue(new MockResponse().setBody(NAKADI_RESPONSE_TWO_ORDERS_ONE_WRONG_TYPE));

        final EventTypeCursor cursor = EventTypeCursor.of(EventTypePartition.of(ORDER_RECEIVED, "0"), "0");
        assertThatThrownBy(() -> getEvent(cursor, OrderReceived.class).toBlocking().value()).isInstanceOf(
                                    InvalidEventTypeException.class).hasMessage(format(
                                        "Unexpected event type (expected=[%s], actual=[%s])", ORDER_RECEIVED.getName(),
                                        WRONG_EVENT_TYPE));
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
