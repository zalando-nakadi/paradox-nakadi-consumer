package de.zalando.paradox.nakadi.consumer.core.http;

public class HttpResponseChunk {

    private final int statusCode;
    private final String content;

    public HttpResponseChunk(final int statusCode, final String content) {
        this.statusCode = statusCode;
        this.content = content;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getContent() {
        return content;
    }
}
