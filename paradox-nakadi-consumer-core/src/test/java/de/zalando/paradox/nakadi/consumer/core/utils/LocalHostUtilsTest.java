package de.zalando.paradox.nakadi.consumer.core.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Ignore;
import org.junit.Test;

public class LocalHostUtilsTest {

    @Test
    public void testGetHostName() {
        final String name = LocalHostUtils.getHostName();
        assertThat(name).isNotEmpty();
    }

    @Test
    @Ignore("Test connects to external service")
    public void testExternalIP() throws MalformedURLException {
        final String name = LocalHostUtils.readResource(new URL("https://api.ipify.org"));
        assertThat(name).isNotEmpty();
    }
}
