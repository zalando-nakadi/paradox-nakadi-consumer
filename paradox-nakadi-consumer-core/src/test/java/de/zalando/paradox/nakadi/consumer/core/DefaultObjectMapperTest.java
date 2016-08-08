package de.zalando.paradox.nakadi.consumer.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import java.time.OffsetDateTime;

import java.util.Date;

import org.joda.time.DateTime;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultObjectMapperTest {

    private static final String DATE_TIME_STRING = "\"2016-07-14T08:34:39.932Z\"";
    private static final long DATE_TIME_MILLIS = 1468485279932L;

    private final ObjectMapper mapper = new DefaultObjectMapper().jacksonObjectMapper();

    @Test
    public void testReadOffsetDateTime() throws IOException {
        final OffsetDateTime offsetDateTime = mapper.readValue(DATE_TIME_STRING, OffsetDateTime.class);
        final Date convertToDate = Date.from(offsetDateTime.toInstant());
        assertThat(convertToDate.getTime()).isEqualTo(DATE_TIME_MILLIS);
    }

    @Test
    public void testReadDateTime() throws IOException {
        final DateTime dateTime = mapper.readValue(DATE_TIME_STRING, DateTime.class);
        assertThat(dateTime.getMillis()).isEqualTo(DATE_TIME_MILLIS);
    }

    @Test
    public void testReadDate() throws IOException {
        final Date date = mapper.readValue(DATE_TIME_STRING, Date.class);
        assertThat(date.getTime()).isEqualTo(DATE_TIME_MILLIS);
    }
}
