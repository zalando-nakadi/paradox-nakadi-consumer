package de.zalando.paradox.nakadi.consumer.boot;

import static org.assertj.core.api.Java6Assertions.assertThat;

import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawContentHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.RawEventHandler;

public class ReplayHandlerTest {
    private static final String ORDER_NUMBER = "24873243241";
    private final ReplayHandler replayHandler = new ReplayHandler();

    private static final String EVENT_TYPE = "order.ORDER_RECEIVED";

    private static final String CONSUMER_NAME = "orderReceivedConsumer";

    private static final EventTypePartition PARTITION = EventTypePartition.of(EventType.of(EVENT_TYPE), "0");
    private static final EventTypeCursor CURSOR = EventTypeCursor.of(PARTITION, "5");

    //J-
    private static final String CONTENT =
        "{\"cursor\":" +
                "{" +
                    "\"partition\":\"0\"," +
                    "\"offset\":\"5\"" +
                "}," +
                "\"events\":" +
                    "[" +
                        "{" +
                            "\"metadata\":" +
                                "{" +
                                    "\"occurred_at\":\"2016-03-15T23:47:15+01:00\"," +
                                    "\"eid\":\"d765de34-09c0-4bbb-8b1e-7160a33a0791\"," +
                                    "\"event_type\":\"order.ORDER_RECEIVED\"," +
                                    "\"received_at\":\"2016-07-14T08:34:39.932Z\"," +
                                    "\"flow_id\":\"TnPBnBGSCIk7lqMcyCTPQoqb\"" +
                                 "}," +
                            "\"order_number\":\"24873243241\"" +
                         "}" +
                    "]" +
        "}\n";
    //J+

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Metadata {

        @JsonProperty("eid")
        UUID eid;

        @JsonProperty("occurred_at")
        Date occurredAt;

        @JsonProperty("event_type")
        String eventType;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class OrderReceived {

        @JsonProperty("metadata")
        Metadata metadata;

        @JsonProperty("order_number")
        String orderNumber;
    }

    @Captor
    private ArgumentCaptor<EventTypeCursor> eventCursorCaptor;

    @Captor
    private ArgumentCaptor<String> stringCaptor;

    @Captor
    private ArgumentCaptor<List<String>> stringsCaptor;

    @Captor
    private ArgumentCaptor<OrderReceived> orderCaptor;

    @Captor
    private ArgumentCaptor<List<OrderReceived>> ordersCaptor;

    @Captor
    private ArgumentCaptor<JsonNode> jsonNodeCaptor;

    @Captor
    private ArgumentCaptor<List<JsonNode>> jsonNodesCaptor;

    @Mock
    private BatchEventsHandler<OrderReceived> batchEventsHandler;

    @Mock
    private BatchEventsBulkHandler<OrderReceived> batchEventsBulkHandler;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(batchEventsHandler.getEventClass()).thenReturn(OrderReceived.class);
        when(batchEventsBulkHandler.getEventClass()).thenReturn(OrderReceived.class);
    }

    @Test
    public void testRawContentHandler() {
        final RawContentHandler handler = Mockito.mock(RawContentHandler.class);
        replayHandler.handle(CONSUMER_NAME,handler, PARTITION, CONTENT);
        verify(handler, times(1)).onEvent(eventCursorCaptor.capture(), stringCaptor.capture());
        assertThat(eventCursorCaptor.getValue()).isEqualTo(CURSOR);
        assertThat(stringCaptor.getValue()).isEqualTo(CONTENT);
    }

    @Test
    public void testBatchEventsHandler() {
        replayHandler.handle(CONSUMER_NAME,batchEventsHandler, PARTITION, CONTENT);
        verify(batchEventsHandler, times(1)).onEvent(eventCursorCaptor.capture(), orderCaptor.capture());
        assertThat(orderCaptor.getValue().metadata.eventType).isEqualTo(EVENT_TYPE);
        assertThat(orderCaptor.getValue().orderNumber).isEqualTo(ORDER_NUMBER);
    }

    @Test
    public void testBatchEventsBulkHandler() {
        replayHandler.handle(CONSUMER_NAME,batchEventsBulkHandler, PARTITION, CONTENT);
        verify(batchEventsBulkHandler, times(1)).onEvent(eventCursorCaptor.capture(), ordersCaptor.capture());
        assertThat(ordersCaptor.getValue()).hasSize(1);
        assertThat(ordersCaptor.getValue()).extracting("orderNumber").containsExactly(ORDER_NUMBER);
    }

    @Test
    public void testRawEventHandler() {
        final RawEventHandler handler = Mockito.mock(RawEventHandler.class);
        replayHandler.handle(CONSUMER_NAME,handler, PARTITION, CONTENT);
        verify(handler, times(1)).onEvent(eventCursorCaptor.capture(), stringCaptor.capture());
        assertThat(stringCaptor.getValue()).startsWith("{\"metadata\":{");
    }

    @Test
    public void testRawEventBulkHandler() {
        final RawEventBulkHandler handler = Mockito.mock(RawEventBulkHandler.class);
        replayHandler.handle(CONSUMER_NAME,handler, PARTITION, CONTENT);
        verify(handler, times(1)).onEvent(eventCursorCaptor.capture(), stringsCaptor.capture());
        assertThat(stringsCaptor.getValue()).hasSize(1);
        assertThat(stringsCaptor.getValue().get(0)).startsWith("{\"metadata\":{");
    }

    @Test
    public void testJsonEventHandler() {
        final JsonEventHandler handler = Mockito.mock(JsonEventHandler.class);
        replayHandler.handle(CONSUMER_NAME,handler, PARTITION, CONTENT);
        verify(handler, times(1)).onEvent(eventCursorCaptor.capture(), jsonNodeCaptor.capture());
        assertThat(jsonNodeCaptor.getValue().get("order_number").textValue()).isEqualTo(ORDER_NUMBER);
    }

    @Test
    public void testJsonEventBulkHandler() {
        final JsonEventBulkHandler handler = Mockito.mock(JsonEventBulkHandler.class);
        replayHandler.handle(CONSUMER_NAME,handler, PARTITION, CONTENT);
        verify(handler, times(1)).onEvent(eventCursorCaptor.capture(), jsonNodesCaptor.capture());
        assertThat(jsonNodesCaptor.getValue()).hasSize(1);
        assertThat(jsonNodesCaptor.getValue().get(0).get("order_number").textValue()).isEqualTo(ORDER_NUMBER);
    }

    @Test
    public void testUnknownHandler() {
        final EventHandler handler = Mockito.mock(EventHandler.class);
        assertThatThrownBy(() -> replayHandler.handle(CONSUMER_NAME,handler, PARTITION, CONTENT)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testQueryCursor() {
        assertThat(replayHandler.getQueryCursor(EventTypeCursor.of(PARTITION, "BEGIN"))).isEqualTo(EventTypeCursor.of(
                PARTITION, "BEGIN"));
        assertThat(replayHandler.getQueryCursor(EventTypeCursor.of(PARTITION, "0"))).isEqualTo(EventTypeCursor.of(
                PARTITION, "BEGIN"));
        assertThat(replayHandler.getQueryCursor(EventTypeCursor.of(PARTITION, "1"))).isEqualTo(EventTypeCursor.of(
                PARTITION, "0"));
        assertThat(replayHandler.getQueryCursor(EventTypeCursor.of(PARTITION, "2"))).isEqualTo(EventTypeCursor.of(
                PARTITION, "1"));
    }
}
