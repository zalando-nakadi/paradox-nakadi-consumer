package de.zalando.paradox.nakadi.consumer.core.http.requests;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import de.zalando.paradox.nakadi.consumer.core.ConsumerConfig;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.http.HttpReactiveHandler;
import de.zalando.paradox.nakadi.consumer.core.http.HttpResponseChunk;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.ResponseHandler;
import de.zalando.paradox.nakadi.consumer.core.http.okhttp.RxHttpRequest;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallback;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.LoggingUtils;

import rx.Observable;

public class HttpGetEventsHandler implements HttpReactiveHandler, PartitionCommitCallback {

    private final Logger log;

    private final ConsumerConfig config;
    private final EventTypeCursor startCursor;
    private HttpGetEvents httpGetEvents;

    private PartitionCoordinator coordinator;
    private ResponseHandler responseHandler;
    private final AtomicBoolean callbackRegistered = new AtomicBoolean(false);

    public HttpGetEventsHandler(final String baseUri, final EventTypeCursor startCursor, final ConsumerConfig config) {
        this.log = LoggingUtils.getLogger(getClass(), startCursor.getEventTypePartition());
        this.startCursor = startCursor;
        this.config = config;
        this.httpGetEvents = new HttpGetEvents(baseUri, startCursor, config.getEventStreamConfig());
        this.coordinator = config.getPartitionCoordinator();
        this.responseHandler = config.getResponseHandlerFactory().get(startCursor.getEventTypePartition(),
                config.getObjectMapper());
    }

    @Override
    public Logger getLogger(final Class<?> clazz) {
        return LoggingUtils.getLogger(getClass(), "events", startCursor.getEventTypePartition());
    }

    @Override
    public void onResponse(final String content) {
        log.trace("ResultCallback : [{}]", content);
        responseHandler.onResponse(content);
    }

    @Override
    public void onErrorResponse(final int statusCode, final String content) {
        log.trace("Error result [{} / {}]", statusCode, content);
        coordinator.error(statusCode, content, startCursor.getEventTypePartition());
    }

    @Override
    public void onStarted() {
        log.trace("Started");

    }

    @Override
    public void onFinished() {
        log.trace("Finished");
        try {
            coordinator.flush(startCursor.getEventTypePartition());
        } finally {
            coordinator.finished(startCursor.getEventTypePartition());
        }

    }

    @Override
    public void init() {
        if (callbackRegistered.compareAndSet(false, true)) {
            coordinator.registerCommitCallback(startCursor.getEventTypePartition(), this);
        }
    }

    @Override
    public void close() {
        if (callbackRegistered.compareAndSet(true, false)) {
            coordinator.unregisterCommitCallback(startCursor.getEventTypePartition());
        }
    }

    @Override
    public long getRetryAfterMillis() {

        // retry after
        if (config.getEventsRetryRandomMillis() > 0) {
            return config.getEventsRetryAfterMillis()
                    + ThreadLocalRandom.current().nextLong(config.getEventsRetryRandomMillis());
        } else {
            return config.getEventsRetryAfterMillis();
        }
    }

    @Override
    public Observable<HttpResponseChunk> createRequest() {
        return new RxHttpRequest(config.getEventsTimeoutMillis(), config.getAuthorizationValueProvider()).createRequest(
                httpGetEvents);
    }

    @Override
    public void onCommitComplete(final EventTypeCursor cursor) {
        log.trace("onCommitComplete");
        httpGetEvents.setOffset(cursor.getOffset());
    }
}
