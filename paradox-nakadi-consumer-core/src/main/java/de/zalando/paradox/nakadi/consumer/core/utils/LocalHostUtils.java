package de.zalando.paradox.nakadi.consumer.core.utils;

import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.CharStreams;

public class LocalHostUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalHostUtils.class);
    private static final String EC2_HOSTNAME_URL = "http://169.254.169.254/latest/meta-data/hostname";

    private static final String HOST_NAME = getLocalHostName();

    public static String getHostName() {
        return HOST_NAME;
    }

    private static String getLocalHostName() {
        final String result = getEC2HostName();
        return !result.isEmpty() ? result : geLocalHostAddress();
    }

    private static String geLocalHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "";
        }
    }

    private static String getEC2HostName() {
        try {
            return readResource(new URL(EC2_HOSTNAME_URL));
        } catch (Exception e) {
            return "";
        }
    }

    static String readResource(final URL url) {
        try {
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(1000 * 2);
            connection.setReadTimeout(1000 * 5);
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.connect();

            if (connection.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                return "";
            }

            try(InputStream is = connection.getInputStream();
                    InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                return CharStreams.toString(reader);
            }
        } catch (IOException e) {
            LOGGER.info("Cannot retrieve EC2 hostname due to [{}]", getMessage(e));
            return "";
        }
    }
}
