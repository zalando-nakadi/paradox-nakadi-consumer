package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Matchers.any;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.EVENT_TYPE_PARTITION;
import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.KEEP_ALIVE_EVENT;
import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.OBJECT_MAPPER;
import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.ONE_EVENT;
import static de.zalando.paradox.nakadi.consumer.core.http.handlers.TestEvents.TWO_EVENTS;

import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class RawContentResponseHandlerTest {

    @Mock
    private PartitionCoordinator coordinator;

    @Mock
    private RawContentHandler delegate;

    private RawContentResponseHandler handler;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.handler = new RawContentResponseHandler(EVENT_TYPE_PARTITION, OBJECT_MAPPER, coordinator, delegate);
    }

    @Test
    public void testOneEvent() {
        handler.onResponse(ONE_EVENT);

        final ArgumentCaptor<EventTypeCursor> eventCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        final ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);

        verify(delegate, times(1)).onEvent(eventCursorCaptor.capture(), contentCaptor.capture());
        assertThat(eventCursorCaptor.getValue()).extracting("eventTypePartition", "offset").containsExactly(
            EVENT_TYPE_PARTITION, "5");

        assertThat(contentCaptor.getValue()).isEqualTo(ONE_EVENT);

        final ArgumentCaptor<EventTypeCursor> coordinatorCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        verify(coordinator, times(1)).commit(coordinatorCursorCaptor.capture());
        assertThat(coordinatorCursorCaptor.getValue()).extracting("eventTypePartition", "offset").containsExactly(
            EVENT_TYPE_PARTITION, "5");
    }

    @Test
    public void testTwoEvents() {
        handler.onResponse(TWO_EVENTS);

        final ArgumentCaptor<EventTypeCursor> eventCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        final ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);

        verify(delegate, times(1)).onEvent(eventCursorCaptor.capture(), contentCaptor.capture());
        assertThat(eventCursorCaptor.getValue()).extracting("eventTypePartition", "offset").containsExactly(
            EVENT_TYPE_PARTITION, "9");

        assertThat(contentCaptor.getValue()).isEqualTo(TWO_EVENTS);

        final ArgumentCaptor<EventTypeCursor> coordinatorCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        verify(coordinator, times(1)).commit(coordinatorCursorCaptor.capture());
        assertThat(coordinatorCursorCaptor.getValue()).extracting("eventTypePartition", "offset").containsExactly(
            EVENT_TYPE_PARTITION, "9");

    }

    @Test
    public void testKeepAlive() {
        handler.onResponse(KEEP_ALIVE_EVENT);

        // payload is not analysed
        final ArgumentCaptor<EventTypeCursor> eventCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        final ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);

        verify(delegate, times(1)).onEvent(eventCursorCaptor.capture(), contentCaptor.capture());
        assertThat(eventCursorCaptor.getValue()).extracting("eventTypePartition", "offset").containsExactly(
            EVENT_TYPE_PARTITION, "9");

        assertThat(contentCaptor.getValue()).isEqualTo(KEEP_ALIVE_EVENT);

        final ArgumentCaptor<EventTypeCursor> coordinatorCursorCaptor = ArgumentCaptor.forClass(EventTypeCursor.class);
        verify(coordinator, times(1)).commit(coordinatorCursorCaptor.capture());
        assertThat(coordinatorCursorCaptor.getValue()).extracting("eventTypePartition", "offset").containsExactly(
            EVENT_TYPE_PARTITION, "9");
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
