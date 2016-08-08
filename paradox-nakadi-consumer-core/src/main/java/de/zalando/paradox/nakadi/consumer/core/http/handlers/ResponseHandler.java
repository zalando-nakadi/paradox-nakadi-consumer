package de.zalando.paradox.nakadi.consumer.core.http.handlers;

public interface ResponseHandler {
    void onResponse(final String content);
}
