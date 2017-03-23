package de.zalando.paradox.nakadi.consumer.boot;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.boot.test.SpringApplicationConfiguration;

import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(
    classes = {
        EventReceiverRegistryConfiguration.class, NakadiConsumerConfiguration.class, NakadiSettingsConfiguration.class,
        NakadiEventErrorHandlerConfiguration.class
    }
)
@TestPropertySource(
    properties = {                                                           //
        "paradox.nakadi.defaults.oauth2Enabled: false",                      //
        "paradox.nakadi.defaults.nakadiUrl: http://localhost:8080",          //
        "paradox.nakadi.defaults.eventsBatchLimit: 100",                     //
        "paradox.nakadi.defaults.eventsBatchTimeoutSeconds: 5",              //
        "paradox.nakadi.consumers.testConsumer.eventsBatchLimit: 1",         //
        "paradox.nakadi.consumers.testConsumer.eventsBatchTimeoutSeconds: 1" //
    }
)
public class EventReceiverRegistryConfigurationIT {

    @Autowired
    private EventReceiverRegistryConfiguration eventReceiverRegistryConfiguration;

    @Test
    public void testShouldPreferConsumerEventsBatchLimit() {
        final Integer eventsBatchLimit = eventReceiverRegistryConfiguration.getEventsBatchLimit("testConsumer");
        assertThat(eventsBatchLimit).isEqualTo(1);
    }

    @Test
    public void testShouldPreferConsumerEventsBatchTimeoutSeconds() {
        final Integer eventsBatchLimit = eventReceiverRegistryConfiguration.getEventsBatchTimeoutSeconds(
                "testConsumer");
        assertThat(eventsBatchLimit).isEqualTo(1);
    }

}
