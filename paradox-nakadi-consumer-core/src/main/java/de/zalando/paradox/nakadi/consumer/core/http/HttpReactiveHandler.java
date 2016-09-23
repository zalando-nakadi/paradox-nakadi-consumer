package de.zalando.paradox.nakadi.consumer.core.http;

import org.slf4j.Logger;

import rx.Observable;

public interface HttpReactiveHandler {

    void init();

    void close();

    Logger getLogger(Class<?> clazz);

    void onResponse(String content);

    void onErrorResponse(int statusCode, String content);

    void onStarted();

    void onFinished();

    long getRetryAfterMillis();

    Observable<HttpResponseChunk> createRequest();
}
