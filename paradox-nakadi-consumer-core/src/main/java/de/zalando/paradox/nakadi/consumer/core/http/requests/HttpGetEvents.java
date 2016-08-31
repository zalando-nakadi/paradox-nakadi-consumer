package de.zalando.paradox.nakadi.consumer.core.http.requests;

import static java.lang.String.format;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import java.util.Collections;
import java.util.Map;

import org.apache.http.client.utils.URIBuilder;

import de.zalando.paradox.nakadi.consumer.core.EventStreamConfig;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.http.HttpGetRequest;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class HttpGetEvents implements HttpGetRequest {

    private static final String HEADER_NAKADI_CURSORS = "X-Nakadi-Cursors";
    private static final String PARTITION_CURSOR = "[{\"partition\":\"%s\", \"offset\":\"%s\"}]";
    private static final String HTTP_GET_EVENTS_URI = "%s/event-types/%s/events";

    private static final String PARAM_STREAM_TIMEOUT = "stream_timeout";
    private static final String PARAM_STREAM_KEEP_ALIVE_LIMIT = "stream_keep_alive_limit";
    private static final String PARAM_BATCH_LIMIT = "batch_limit";
    private static final String PARAM_BATCH_FLUSH_TIMEOUT = "batch_flush_timeout";
    private static final String PARAM_STREAM_LIMIT = "stream_limit";

    private final String baseUrl;
    private final String eventType;
    private final String partition;
    private final EventStreamConfig eventStreamConfig;

    // BEGIN , 0 , 1, ...
    private volatile String offset;

    public HttpGetEvents(final String baseUrl, final EventTypeCursor cursor,
            final EventStreamConfig eventStreamConfig) {
        this(baseUrl, cursor.getName(), cursor.getPartition(), cursor.getOffset(), eventStreamConfig);
    }

    public HttpGetEvents(final String baseUrl, final String eventType, final String partition, final String offset,
            final EventStreamConfig eventStreamConfig) {
        this.baseUrl = baseUrl;
        this.eventType = eventType;
        this.partition = partition;
        this.offset = offset;
        this.eventStreamConfig = eventStreamConfig;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(final String offset) {
        this.offset = offset;
    }

    private static URL getURL(final String baseUrl, final String eventType, final EventStreamConfig eventStreamConfig) {
        try {
            final URIBuilder builder = new URIBuilder(format(HTTP_GET_EVENTS_URI, baseUrl, eventType));
            addParams(builder, eventStreamConfig);
            return builder.build().toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            ThrowableUtils.throwException(e);
            return null;
        }
    }

    private static URIBuilder addParams(final URIBuilder builder, final EventStreamConfig eventStreamConfig) {
        if (null != eventStreamConfig) {
            if (null != eventStreamConfig.getStreamTimeoutSeconds()) {
                builder.addParameter(PARAM_STREAM_TIMEOUT, eventStreamConfig.getStreamTimeoutSeconds().toString());
            }

            if (null != eventStreamConfig.getStreamKeepAliveLimit()) {
                builder.addParameter(PARAM_STREAM_KEEP_ALIVE_LIMIT,
                    eventStreamConfig.getStreamKeepAliveLimit().toString());
            }

            if (null != eventStreamConfig.getBatchLimit()) {
                builder.addParameter(PARAM_BATCH_LIMIT, eventStreamConfig.getBatchLimit().toString());
            }

            if (null != eventStreamConfig.getBatchTimeoutSeconds()) {
                builder.addParameter(PARAM_BATCH_FLUSH_TIMEOUT, eventStreamConfig.getBatchTimeoutSeconds().toString());
            }

            if (null != eventStreamConfig.getStreamLimit()) {
                builder.addParameter(PARAM_STREAM_LIMIT, eventStreamConfig.getStreamLimit().toString());
            }
        }

        return builder;
    }

    @Override
    public URL getUrl() {
        return getURL(baseUrl, eventType, eventStreamConfig);
    }

    @Override
    public Map<String, String> getHeaders() {
        return Collections.singletonMap(HEADER_NAKADI_CURSORS, format(PARTITION_CURSOR, partition, offset));
    }
}
