package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SQSConfigTest {

    @Test
    public void testShouldSetQueueUrl() {
        final String queueUrl = "https://example.org";
        assertThat(new SQSConfig.Builder().queueUrl(queueUrl).build().getQueueUrl()).isEqualTo(queueUrl);
    }

}
