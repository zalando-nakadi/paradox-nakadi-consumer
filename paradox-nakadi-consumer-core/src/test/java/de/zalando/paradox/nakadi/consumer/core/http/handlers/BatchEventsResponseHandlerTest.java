package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Matchers.any;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.EVENT_TYPE_PARTITION;
import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.KEEP_ALIVE_EVENT;
import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.OBJECT_MAPPER;
import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.ONE_EVENT;
import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.THREE_EVENTS;
import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.TWO_EVENTS;

import java.util.UUID;

import org.assertj.core.groups.Tuple;

import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.testdomain.OrderReceived;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class BatchEventsResponseHandlerTest {

    @Mock
    private PartitionCoordinator coordinator;

    @Mock
    private BatchEventsHandler<OrderReceived> delegate;

    private BatchEventsResponseHandler<OrderReceived> handler;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(delegate.getEventClass()).thenReturn(OrderReceived.class);
        this.handler = new BatchEventsResponseHandler<>(EVENT_TYPE_PARTITION, OBJECT_MAPPER, coordinator, delegate);
    }

    @Test
    public void testOneEvent() {

        handler.onResponse(ONE_EVENT);

        final ArgumentCaptor<EventTypeCursor> eventCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        final ArgumentCaptor<OrderReceived> orderReceivedCaptor = ArgumentCaptor.forClass(OrderReceived.class);
        verify(delegate, times(1)).onEvent(eventCursorCaptor.capture(), orderReceivedCaptor.capture());
        assertThat(eventCursorCaptor.getValue()).extracting("eventTypePartition", "offset").containsExactly(
            EVENT_TYPE_PARTITION, "5");

        assertThat(orderReceivedCaptor.getValue()).extracting("orderNumber").containsExactly("24873243241");

        //J-
        assertThat(orderReceivedCaptor.getValue().getMetadata())
                .extracting("eid", "eventType", "flowId")
                .containsExactly(UUID.fromString("d765de34-09c0-4bbb-8b1e-7160a33a0791"), "order.ORDER_RECEIVED", "TnPBnBGSCIk7lqMcyCTPQoqb");
        //J+
        final ArgumentCaptor<EventTypeCursor> coordinatorCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        verify(coordinator, times(1)).commit(coordinatorCursorCaptor.capture());
        assertThat(coordinatorCursorCaptor.getValue()).extracting("eventTypePartition", "offset").containsExactly(
            EVENT_TYPE_PARTITION, "5");

    }

    @Test
    public void testTwoEvents() {
        handler.onResponse(TWO_EVENTS);

        final ArgumentCaptor<EventTypeCursor> eventCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        final ArgumentCaptor<OrderReceived> orderReceivedCaptor = ArgumentCaptor.forClass(OrderReceived.class);
        verify(delegate, times(2)).onEvent(eventCursorCaptor.capture(), orderReceivedCaptor.capture());

        assertThat(orderReceivedCaptor.getAllValues()).extracting("orderNumber").containsExactly("24873243241",
            "24873243242");

        final ArgumentCaptor<EventTypeCursor> coordinatorCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        verify(coordinator, times(1)).commit(coordinatorCursorCaptor.capture());

        assertThat(coordinatorCursorCaptor.getAllValues()).extracting("eventTypePartition", "offset").containsExactly(
            Tuple.tuple(EVENT_TYPE_PARTITION, "9"));
    }

    @Test
    public void testThreeEvents() {
        handler.onResponse(THREE_EVENTS);

        final ArgumentCaptor<EventTypeCursor> eventCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        final ArgumentCaptor<OrderReceived> orderReceivedCaptor = ArgumentCaptor.forClass(OrderReceived.class);
        verify(delegate, times(3)).onEvent(eventCursorCaptor.capture(), orderReceivedCaptor.capture());

        assertThat(orderReceivedCaptor.getAllValues()).extracting("orderNumber").containsExactly("24873243241",
            "24873243242", "24873243243");

        final ArgumentCaptor<EventTypeCursor> coordinatorCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        verify(coordinator, times(1)).commit(coordinatorCursorCaptor.capture());

        assertThat(coordinatorCursorCaptor.getAllValues()).extracting("eventTypePartition", "offset").containsExactly(
            Tuple.tuple(EVENT_TYPE_PARTITION, "9"));
    }

    @Test
    public void testKeepAlive() {
        handler.onResponse(KEEP_ALIVE_EVENT);
        verify(delegate, times(0)).onEvent(any(), any());
        verify(coordinator, times(0)).commit(any(EventTypeCursor.class));
    }

    @Test
    public void testOnResponseError() {
        doThrow(new RuntimeException("Test processing error")).when(delegate).onEvent(any(), any());

        handler.onResponse(ONE_EVENT);

        verify(delegate, times(1)).onEvent(any(), any());
        verify(coordinator, times(1)).commit(any());
        verify(coordinator, times(1)).error(any(), any(), any(), any());
    }
}
