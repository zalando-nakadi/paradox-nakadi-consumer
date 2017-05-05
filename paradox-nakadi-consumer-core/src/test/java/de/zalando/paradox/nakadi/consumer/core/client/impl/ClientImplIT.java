package de.zalando.paradox.nakadi.consumer.core.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.Iterables;

import de.zalando.paradox.nakadi.consumer.core.client.Client;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

public class ClientImplIT {

    private static final EventType TEST_EVENT_TYPE = EventType.of("test-event");

    private MockWebServer mockWebServer;

    private Client client;

    @Before
    public void setUp() throws Exception {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        client = new ClientImpl.Builder(mockWebServer.url("/").toString()).withAuthorization(() -> "test")
                                                                          .withObjectMapper(new ObjectMapper()).build();
    }

    @Test
    public void testShouldReturnNumberOfUnconsumedEvents() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse().setBody(
                "[{\"partition\": \"0\", \"oldest_available_offset\" : \"0\", \"newest_available_offset\": \"838282\", \"unconsumed_events\": 838282}]"));

        final List<NakadiPartition> nakadiPartitions = client.getCursorsLag(Collections.singletonList(
                                                                     EventTypeCursor.of(
                                                                         EventTypePartition.of(TEST_EVENT_TYPE, "0"),
                                                                         "BEGIN"))).toBlocking().value();

        final NakadiPartition nakadiPartition = Iterables.getOnlyElement(nakadiPartitions);
        assertThat(nakadiPartition.getOldestAvailableOffset()).isEqualTo("0");
        assertThat(nakadiPartition.getNewestAvailableOffset()).isEqualTo("838282");
        assertThat(nakadiPartition.getUnconsumedEvents()).contains(838282L);
    }

    @Test
    public void testShouldReturnExceptionIfNumberOfUnconsumedEventsCannotBeFetched() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(500).setBody("expected error"));
        assertThatThrownBy(() -> {
                                      client.getCursorsLag(
                                          Collections.singletonList(
                                              EventTypeCursor.of(EventTypePartition.of(TEST_EVENT_TYPE, "0"), "BEGIN")))
                                          .toBlocking().value();
                                  }).isInstanceOf(RuntimeException.class).hasMessage("expected error");
    }

    @Test
    public void testShouldAddAuthorizationHeader() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse().setBody(
                "[{\"partition\": \"0\", \"oldest_available_offset\" : \"0\", \"newest_available_offset\": \"838282\", \"unconsumed_events\": 838282}]"));

        client.getCursorsLag(Collections.singletonList(
                      EventTypeCursor.of(EventTypePartition.of(TEST_EVENT_TYPE, "0"), "BEGIN"))).toBlocking().value();

        final RecordedRequest request = mockWebServer.takeRequest();
        assertThat(request.getHeader("Authorization")).isEqualTo("test");
    }
}
