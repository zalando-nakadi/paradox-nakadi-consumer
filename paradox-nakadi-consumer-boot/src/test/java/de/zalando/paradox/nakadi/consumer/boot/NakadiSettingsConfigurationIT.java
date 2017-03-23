package de.zalando.paradox.nakadi.consumer.boot;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.boot.test.SpringApplicationConfiguration;

import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = NakadiSettingsConfiguration.class)
@TestPropertySource(
    properties = {                                                  //
        "paradox.nakadi.defaults.eventsBatchLimit: 100",            //
        "paradox.nakadi.consumers.testConsumer.eventsBatchLimit: 1" //
    }
)
public class NakadiSettingsConfigurationIT {

    @Autowired
    private NakadiSettings nakadiSettings;

    @Test
    public void testShouldSetEventsBatchLimit() {
        final Integer eventsBatchLimit = nakadiSettings.getDefaults().getEventsBatchLimit();
        assertThat(eventsBatchLimit).isEqualTo(100);
    }

    @Test
    public void testShouldSetConsumerEventsBatchLimit() {
        final Integer eventsBatchLimit = nakadiSettings.getConsumers().get("testConsumer").getEventsBatchLimit();
        assertThat(eventsBatchLimit).isEqualTo(1);
    }

}
