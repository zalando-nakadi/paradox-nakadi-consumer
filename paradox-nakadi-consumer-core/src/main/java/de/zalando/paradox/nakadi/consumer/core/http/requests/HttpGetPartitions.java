package de.zalando.paradox.nakadi.consumer.core.http.requests;

import static java.lang.String.format;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import java.util.Collections;
import java.util.Map;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.http.HttpGetRequest;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

public class HttpGetPartitions implements HttpGetRequest {
    private static final String HTTP_PARTITIONS_EVENTS_URI = "%s/event-types/%s/partitions";

    private final String baseUrl;
    private final String eventType;

    public HttpGetPartitions(final String baseUrl, final EventType eventType) {
        this(baseUrl, eventType.getName());
    }

    private HttpGetPartitions(final String baseUrl, final String eventType) {
        this.baseUrl = baseUrl;
        this.eventType = eventType;
    }

    private static URL getURL(final String baseUrl, final String eventType) {
        try {
            return URI.create(format(HTTP_PARTITIONS_EVENTS_URI, baseUrl, eventType)).toURL();
        } catch (MalformedURLException e) {
            ThrowableUtils.throwException(e);
            return null;
        }
    }

    @Override
    public URL getUrl() {
        return getURL(baseUrl, eventType);
    }

    @Override
    public Map<String, String> getHeaders() {
        return Collections.emptyMap();
    }
}
