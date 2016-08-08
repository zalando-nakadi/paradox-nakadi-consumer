package de.zalando.paradox.nakadi.consumer.core.http;

import java.net.URL;
import java.util.Map;

public interface HttpGetRequest {

    URL getUrl();

    Map<String, String> getHeaders();
}
