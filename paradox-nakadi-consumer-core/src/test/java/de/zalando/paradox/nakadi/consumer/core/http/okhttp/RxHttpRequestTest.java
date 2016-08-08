package de.zalando.paradox.nakadi.consumer.core.http.okhttp;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomUtils.nextInt;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Mockito.when;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.net.ServerSocket;
import java.net.URL;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;

import org.assertj.core.groups.Tuple;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import de.zalando.paradox.nakadi.consumer.core.http.HttpGetRequest;
import de.zalando.paradox.nakadi.consumer.core.http.HttpResponseChunk;

import rx.Observable;

import rx.observers.TestSubscriber;

public class RxHttpRequestTest {

    private static final int PORT = findFreePort(10000);
    private static final String EVENTS_RESOURCE = "/event-types/order.ORDER_RECEIVED/events";
    private static final String NAKADI_URL = "http://localhost:" + PORT + EVENTS_RESOURCE;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(PORT);

    @Mock
    private HttpGetRequest mockRequestProducer;

    private RxHttpRequest rxHttpRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        WireMock.reset();
        when(mockRequestProducer.getUrl()).thenReturn(new URL(NAKADI_URL));
        when(mockRequestProducer.getHeaders()).thenReturn(Collections.emptyMap());
        rxHttpRequest = new RxHttpRequest(TimeUnit.SECONDS.toMillis(60), null);
    }

    @Test
    public void testEmptyStreamOK() throws Exception {
        stubFor(get(urlEqualTo(EVENTS_RESOURCE)).willReturn(aResponse().withStatus(200).withBody("")));

        final Observable<HttpResponseChunk> observable = rxHttpRequest.createRequest(mockRequestProducer);
        final TestSubscriber<HttpResponseChunk> testSubscriber = new TestSubscriber<>();
        observable.subscribe(testSubscriber);
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
        WireMock.verify(1, getRequestedFor(urlEqualTo(EVENTS_RESOURCE)));
    }

    @Test
    public void testEmptyStreamForbidden() throws Exception {
        stubFor(get(urlEqualTo(EVENTS_RESOURCE)).willReturn(aResponse().withStatus(403).withBody("")));

        final Observable<HttpResponseChunk> observable = rxHttpRequest.createRequest(mockRequestProducer);
        final TestSubscriber<HttpResponseChunk> testSubscriber = new TestSubscriber<>();
        observable.subscribe(testSubscriber);
        testSubscriber.assertNoErrors();
        WireMock.verify(1, getRequestedFor(urlEqualTo(EVENTS_RESOURCE)));

        final List<HttpResponseChunk> chunks = testSubscriber.getOnNextEvents();
        assertThat(chunks).extracting("statusCode", "content").containsOnly(Tuple.tuple(403, ""));
    }

    @Test
    public void testStream1Event() throws Exception {
        final String body = "TEST";
        stubFor(get(urlEqualTo(EVENTS_RESOURCE)).willReturn(aResponse().withStatus(200).withBody(body)));

        final Observable<HttpResponseChunk> observable = rxHttpRequest.createRequest(mockRequestProducer);
        final TestSubscriber<HttpResponseChunk> testSubscriber = new TestSubscriber<>();
        observable.subscribe(testSubscriber);
        testSubscriber.assertNoErrors();
        WireMock.verify(1, getRequestedFor(urlEqualTo(EVENTS_RESOURCE)));

        final List<HttpResponseChunk> chunks = testSubscriber.getOnNextEvents();
        assertThat(chunks).extracting("statusCode", "content").containsOnly(Tuple.tuple(200, body));
    }

    @Test
    public void testStreamMultipleEvent() throws Exception {
        final String[] events = getEvents();
        final String body = Arrays.stream(events).collect(Collectors.joining(RxHttpRequest.BATCH_SPLITTER));
        stubFor(get(urlEqualTo(EVENTS_RESOURCE)).willReturn(aResponse().withStatus(200).withBody(body)));

        final Observable<HttpResponseChunk> observable = rxHttpRequest.createRequest(mockRequestProducer);
        final TestSubscriber<HttpResponseChunk> testSubscriber = new TestSubscriber<>();
        observable.subscribe(testSubscriber);
        testSubscriber.assertNoErrors();
        WireMock.verify(1, getRequestedFor(urlEqualTo(EVENTS_RESOURCE)));

        final List<HttpResponseChunk> chunks = testSubscriber.getOnNextEvents();
        assertThat(chunks).hasSize(events.length);
        for (int i = 0; i < events.length; i++) {
            assertThat(chunks.get(i)).extracting("statusCode", "content").containsOnly(200, events[i]);
        }
    }

    @Test
    public void testCursorAndAuthorization() throws Exception {

        final String headerName = "X-Nakadi-Cursors";
        final String headerValue = "[{\"partition\": \"0\", \"offset\":\"BEGIN\"}]";
        final String authorization = "Bearer b09bb129-3820-4178-af1e-1158a5464b56";
        final String body = "TEST";

        when(mockRequestProducer.getHeaders()).thenReturn(Collections.singletonMap(headerName, headerValue));
        rxHttpRequest = new RxHttpRequest(TimeUnit.MINUTES.toMillis(10), () -> authorization);

        stubFor(get(urlEqualTo(EVENTS_RESOURCE)).withHeader("Authorization", equalTo(authorization)).withHeader(
                headerName, equalTo(headerValue)).willReturn(aResponse().withStatus(200).withBody(body)));

        final Observable<HttpResponseChunk> observable = rxHttpRequest.createRequest(mockRequestProducer);
        final TestSubscriber<HttpResponseChunk> testSubscriber = new TestSubscriber<>();
        observable.subscribe(testSubscriber);
        testSubscriber.assertNoErrors();

        final List<HttpResponseChunk> chunks = testSubscriber.getOnNextEvents();
        assertThat(chunks).hasSize(1);
        assertThat(chunks).extracting("statusCode", "content").containsOnly(Tuple.tuple(200, body));
        WireMock.verify(1, getRequestedFor(urlEqualTo(EVENTS_RESOURCE)));
    }

    @Test
    public void testGzippedResponse() throws Exception {
        final String[] events = getEvents();

        final String content = Arrays.stream(events).collect(Collectors.joining(RxHttpRequest.BATCH_SPLITTER));
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final GZIPOutputStream gzip = new GZIPOutputStream(baos);
        IOUtils.copy(new ByteArrayInputStream(content.getBytes()), gzip);
        gzip.close();

        final byte[] body = baos.toByteArray();
        stubFor(get(urlEqualTo(EVENTS_RESOURCE)).willReturn(
                aResponse().withStatus(200).withHeader("Content-Encoding", "gzip").withBody(body)));

        final Observable<HttpResponseChunk> observable = rxHttpRequest.createRequest(mockRequestProducer);
        final TestSubscriber<HttpResponseChunk> testSubscriber = new TestSubscriber<>();
        observable.subscribe(testSubscriber);
        testSubscriber.assertNoErrors();
        WireMock.verify(1, getRequestedFor(urlEqualTo(EVENTS_RESOURCE)));

        final List<HttpResponseChunk> chunks = testSubscriber.getOnNextEvents();
        assertThat(chunks).hasSize(events.length);
        assertThat(chunks).hasSize(events.length);
        for (int i = 0; i < events.length; i++) {
            assertThat(chunks.get(i)).extracting("statusCode", "content").containsOnly(200, events[i]);
        }
    }

    @Test
    public void testSocketReadException() throws Exception {
        final int timeout = (int) TimeUnit.SECONDS.toMillis(2);
        rxHttpRequest = new RxHttpRequest(timeout, null);

        stubFor(get(urlEqualTo(EVENTS_RESOURCE)).willReturn(
                aResponse().withFixedDelay(timeout + 2000).withStatus(200).withBody("")));

        final Observable<HttpResponseChunk> observable = rxHttpRequest.createRequest(mockRequestProducer);
        final TestSubscriber<HttpResponseChunk> testSubscriber = new TestSubscriber<>();
        observable.subscribe(testSubscriber);
        testSubscriber.assertError(java.net.SocketTimeoutException.class);
        WireMock.verify(1, getRequestedFor(urlEqualTo(EVENTS_RESOURCE)));

    }

    private String[] getEvents() {
        final int n = nextInt(10, 20);
        final String[] events = new String[n * 3];

        for (int i = 0; i < events.length; i = i + 3) {
            events[i] = randomAlphabetic(nextInt(1, 1024));
            events[i + 1] = randomAlphabetic(nextInt(1024, 1024 * 8));
            events[i + 2] = randomAlphabetic(nextInt(1024 * 8, 1024 * 32));
        }

        return events;
    }

    private static int findFreePort(final int from) {
        for (int i = from; i < 65535; i++) {
            try(ServerSocket socket = new ServerSocket(i)) {
                return socket.getLocalPort();
            } catch (java.net.BindException ignore) { }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        throw new IllegalStateException("No free port found from range:" + from);
    }

}
