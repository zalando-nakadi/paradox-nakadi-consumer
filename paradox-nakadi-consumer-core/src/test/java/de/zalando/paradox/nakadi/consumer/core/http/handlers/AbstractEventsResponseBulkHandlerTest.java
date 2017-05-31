package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;

public class AbstractEventsResponseBulkHandlerTest {

    private static final EventTypePartition EVENT_TYPE_PARTITION = EventTypePartition.of(EventType.of("test-event"),
            "0");

    private static final String TEST_CONSUMER_NAME = "test-consumer";

    @Mock
    private PartitionCoordinator mockPartitionCoordinator;

    @Mock
    private EventHandler<List<String>> mockEventHandler;

    @Captor
    private ArgumentCaptor<Throwable> throwableCaptor;

    @Captor
    private ArgumentCaptor<EventTypePartition> eventTypePartitionCaptor;

    @Captor
    private ArgumentCaptor<String> offsetCaptor;

    @Captor
    private ArgumentCaptor<String> eventCaptor;

    @Captor
    private ArgumentCaptor<String> consumerCaptor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testShouldCallCoordinatorIfEventDeserializationFails() {
        final AbstractEventsResponseBulkHandler<String> handler = new AbstractEventsResponseBulkHandler<String>(
                TEST_CONSUMER_NAME, EVENT_TYPE_PARTITION, mockPartitionCoordinator,
                AbstractEventsResponseBulkHandler.class, new ObjectMapper(), (cursor, events) -> { }) {
            @Override
            NakadiEventBatch<String> getEventBatch(final String event) {
                throw new RuntimeException("expected");
            }
        };

        final String eventContent = "{\"cursor\":{\"partition\":\"0\",\"offset\":\"4\"},"
                + "\"events\":[{\"order_number\": \"ORDER_001\", "
                + "\"metadata\": {\"eid\": \"4ae5011e-eb01-11e5-8b4a-1c6f65464fc6\", \"occurred_at\": \"2016-03-15T23:56:11+01:00\"}}]}";
        handler.onResponse(eventContent);

        verify(mockPartitionCoordinator).error(consumerCaptor.capture(), throwableCaptor.capture(),
            eventTypePartitionCaptor.capture(), offsetCaptor.capture(), eventCaptor.capture());

        assertThat(consumerCaptor.getValue()).isEqualTo(TEST_CONSUMER_NAME);
        assertThat(throwableCaptor.getValue()).isInstanceOf(RuntimeException.class).hasMessage("expected");
        assertThat(eventTypePartitionCaptor.getValue()).isEqualTo(EVENT_TYPE_PARTITION);
        assertThat(offsetCaptor.getValue()).isNull();
        assertThat(eventCaptor.getValue()).isEqualTo(eventContent);
    }

    @Test
    public void testShouldCallCoordinatorOnEventHandlerError() {
        doThrow(new RuntimeException("expected")).when(mockEventHandler).onEvent(any(EventTypeCursor.class),
            anyListOf(String.class));

        final AbstractEventsResponseBulkHandler<String> handler = new AbstractEventsResponseBulkHandler<String>(
                TEST_CONSUMER_NAME, EVENT_TYPE_PARTITION, mockPartitionCoordinator,
                AbstractEventsResponseBulkHandler.class, new ObjectMapper(), mockEventHandler) {
            @Override
            NakadiEventBatch<String> getEventBatch(final String event) {
                return new NakadiEventBatch<>(new NakadiCursor("0", "BEGIN"),
                        Collections.singletonList(
                            "{\"order_number\": \"ORDER_001\", " + "\"metadata\": "
                                + "{\"eid\": \"4ae5011e-eb01-11e5-8b4a-1c6f65464fc6\", "
                                + "\"occurred_at\": \"2016-03-15T23:56:11+01:00\"}}"));
            }
        };

        final String eventContent = "{\"cursor\":{\"partition\":\"0\",\"offset\":\"BEGIN\"},"
                + "\"events\":[{\"order_number\": \"ORDER_001\", "
                + "\"metadata\": {\"eid\": \"4ae5011e-eb01-11e5-8b4a-1c6f65464fc6\", \"occurred_at\": \"2016-03-15T23:56:11+01:00\"}}]}";
        handler.onResponse(eventContent);

        verify(mockPartitionCoordinator).error(consumerCaptor.capture(), throwableCaptor.capture(),
            eventTypePartitionCaptor.capture(), offsetCaptor.capture(), eventCaptor.capture());

        assertThat(consumerCaptor.getValue()).isEqualTo(TEST_CONSUMER_NAME);
        assertThat(throwableCaptor.getValue()).isInstanceOf(RuntimeException.class).hasMessage("expected");
        assertThat(eventTypePartitionCaptor.getValue()).isEqualTo(EVENT_TYPE_PARTITION);
        assertThat(offsetCaptor.getValue()).isEqualTo("BEGIN");
        assertThat(eventCaptor.getValue()).isEqualTo(eventContent);
    }

}
