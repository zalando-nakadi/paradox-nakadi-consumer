package de.zalando.paradox.nakadi.consumer.core.http;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import java.net.SocketTimeoutException;

import java.rmi.ConnectException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import rx.Observable;

import rx.schedulers.Schedulers;

public class HttpReactiveReceiverTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpReactiveReceiverTest.class);

    private static final long DEFAULT_RETRY_AFTER = TimeUnit.SECONDS.toMillis(120);

    @Mock
    private HttpReactiveHandler mockHandler;

    private HttpReactiveReceiver receiver;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        doReturn(LOGGER).when(mockHandler).getLogger(anyObject());
        when(mockHandler.getRetryAfterMillis()).thenReturn(DEFAULT_RETRY_AFTER);

        // use immediate scheduler for testing
        receiver = new HttpReactiveReceiver(mockHandler, Schedulers.immediate());
    }

    @Test
    public void testEmitEvent() throws IOException {
        doReturn(emitChunks(200, 1, 1)).when(mockHandler).createRequest();
        receiver.init();

        final ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockHandler, times(1)).init();
        verify(mockHandler, times(1)).onStarted();
        verify(mockHandler, times(1)).onResponse(responseCaptor.capture());
        assertThat(responseCaptor.getValue()).isEqualTo("CONTENT-1-1");

        verify(mockHandler, times(1)).onFinished();
        assertThat(receiver.isRunning()).isTrue();
        assertThat(receiver.isSubscribed()).isTrue();

        receiver.close();
        assertThat(receiver.isRunning()).isFalse();
        assertThat(receiver.isSubscribed()).isFalse();
    }

    @Test
    public void testEmitError() throws IOException {
        final int statusCode = 500;
        doReturn(emitChunks(statusCode, 1, 1)).when(mockHandler).createRequest();
        receiver.init();

        final ArgumentCaptor<Integer> codeCaptor = ArgumentCaptor.forClass(Integer.class);
        final ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockHandler, times(1)).init();
        verify(mockHandler, times(1)).onStarted();
        verify(mockHandler, times(1)).onErrorResponse(codeCaptor.capture(), responseCaptor.capture());

        assertThat(codeCaptor.getValue()).isEqualTo(statusCode);
        assertThat(responseCaptor.getValue()).isEqualTo("CONTENT-1-1");

        verify(mockHandler, times(1)).onFinished();
        assertThat(receiver.isRunning()).isTrue();
        assertThat(receiver.isSubscribed()).isTrue();

        receiver.close();
        assertThat(receiver.isRunning()).isFalse();
        assertThat(receiver.isSubscribed()).isFalse();
    }

    @Test
    public void testRepeatRunningAfterCompletion() throws IOException, InterruptedException {
        when(mockHandler.createRequest()).thenReturn(emitChunks(200, 2, 1));
        testRestart();
    }

    @Test
    public void testRetryRunningAfterError() throws IOException, InterruptedException {
        when(mockHandler.createRequest()).thenReturn(emitChunksWithError(200, 2, 1));
        testRestart();
    }

    private void testRestart() throws InterruptedException, IOException {
        final long timeout = 2000L;
        when(mockHandler.getRetryAfterMillis()).thenReturn(timeout);
        receiver.init();

        final ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockHandler, times(1)).init();
        verify(mockHandler, times(1)).onStarted();
        verify(mockHandler, times(1)).onResponse(responseCaptor.capture());
        assertThat(responseCaptor.getValue()).isEqualTo("CONTENT-1-1");
        verify(mockHandler, times(1)).onFinished();
        assertThat(receiver.isRunning()).isTrue();
        assertThat(receiver.isSubscribed()).isTrue();

        Thread.sleep(timeout + 500L);

        verify(mockHandler, times(1)).init();
        verify(mockHandler, times(2)).onStarted();
        verify(mockHandler, times(2)).onResponse(responseCaptor.capture());
        assertThat(responseCaptor.getValue()).isEqualTo("CONTENT-2-1");
        verify(mockHandler, times(2)).onFinished();

        receiver.close();
        assertThat(receiver.isRunning()).isFalse();
        assertThat(receiver.isSubscribed()).isFalse();
    }

    @Test
    public void testErrorInResponseHandler() throws IOException, InterruptedException {
        final long timeout = 2000L;

        when(mockHandler.getRetryAfterMillis()).thenReturn(timeout);
        when(mockHandler.createRequest()).thenReturn(emitChunks(200, 2, 3));

        doNothing().doThrow(new RuntimeException("Test-Exception-1")).doNothing().when(mockHandler).onResponse(any());

        receiver.init();

        final ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockHandler, times(1)).init();
        verify(mockHandler, times(1)).onStarted();
        verify(mockHandler, times(2)).onResponse(responseCaptor.capture());
        assertThat(responseCaptor.getAllValues()).containsExactly("CONTENT-1-1", "CONTENT-1-2");
        verify(mockHandler, times(1)).onFinished();
        assertThat(receiver.isRunning()).isTrue();
        assertThat(receiver.isSubscribed()).isFalse();

        Thread.sleep(timeout + 500L);

        verify(mockHandler, times(1)).onStarted();
        assertThat(receiver.isRunning()).isTrue();
        assertThat(receiver.isSubscribed()).isFalse();

        receiver.close();
        assertThat(receiver.isRunning()).isFalse();
        assertThat(receiver.isSubscribed()).isFalse();
    }

    private Observable<HttpResponseChunk> emitChunks(final int statusCode, final int restarts, final int events) {
        final AtomicInteger restartCounter = new AtomicInteger(0);
        return Observable.defer(() -> emitRange(statusCode, restarts, events, restartCounter.incrementAndGet()));
    }

    private Observable<HttpResponseChunk> emitChunksWithError(final int statusCode, final int restarts,
            final int events) {
        final AtomicInteger restartCounter = new AtomicInteger(0);
        return Observable.defer(() -> {
                final int restart = restartCounter.incrementAndGet();
                final Observable<HttpResponseChunk> chunks = emitRange(statusCode, restarts, events, restart);
                final Throwable t = restart % 2 == 0 ? new SocketTimeoutException("Test Socket timeout")
                                                     : new ConnectException("Test Connection failed");
                return Observable.concat(chunks, Observable.error(t));
            });
    }

    private Observable<HttpResponseChunk> emitRange(final int statusCode, final int restarts, final int events,
            final int restart) {
        Preconditions.checkState(restart <= restarts, "Max restarts " + restarts);
        return Observable.range(1, events).map(event -> {
                final String content = "CONTENT-" + restart + "-" + event;
                LOGGER.info("Emitting [{}]", content);
                return new HttpResponseChunk(statusCode, content);
            });
    }

}
