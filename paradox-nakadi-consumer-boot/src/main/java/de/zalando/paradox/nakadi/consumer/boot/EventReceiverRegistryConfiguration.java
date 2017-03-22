package de.zalando.paradox.nakadi.consumer.boot;

import static com.google.common.base.MoreObjects.firstNonNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerEventConfigList;
import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerPartitionCoordinatorProvider;
import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;
import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;

@Configuration
public class EventReceiverRegistryConfiguration {

    @Autowired
    private NakadiSettings nakadiSettings;

    @Autowired
    private ConsumerPartitionCoordinatorProvider coordinatorProvider;

    @Autowired
    private ConsumerEventConfigList consumerEventConfigList;

    @Autowired(required = false)
    private AuthorizationValueProvider authorizationValueProvider;

    @Autowired(required = false)
    @Qualifier("nakadiObjectMapper")
    private ObjectMapper objectMapper;

    @Bean
    public EventReceiverRegistry eventReceiverRegistry(final EventReceiverRegistryConfiguration eventReceiverConfig) {
        return new EventReceiverRegistry(eventReceiverConfig, objectMapper);
    }

    public ConsumerPartitionCoordinatorProvider getCoordinatorProvider() {
        return coordinatorProvider;
    }

    public ConsumerEventConfigList getConsumerEventConfigList() {
        return consumerEventConfigList;
    }

    public AuthorizationValueProvider getAuthorizationValueProvider() {
        return authorizationValueProvider;
    }

    public String getNakadiUrl() {
        return nakadiSettings.getDefaults().getNakadiUrl();
    }

    public boolean isEventTypePartitionCoordinator() {
        return nakadiSettings.getDefaults().isEventTypePartitionCoordinator();
    }

    public Long getPartitionsRetryAfterMillis() {
        return nakadiSettings.getDefaults().getPartitionsRetryAfterMillis();
    }

    public Long getPartitionsRetryRandomMillis() {
        return nakadiSettings.getDefaults().getPartitionsRetryRandomMillis();
    }

    public Long getPartitionsTimeoutMillis() {
        return nakadiSettings.getDefaults().getPartitionsTimeoutMillis();
    }

    public Long getEventsTimeoutMillis() {
        return nakadiSettings.getDefaults().getEventsTimeoutMillis();
    }

    public Long getEventsRetryAfterMillis() {
        return nakadiSettings.getDefaults().getEventsRetryAfterMillis();
    }

    public Long getEventsRetryRandomMillis() {
        return nakadiSettings.getDefaults().getEventsRetryRandomMillis();
    }

    public Integer getEventsStreamTimeoutSeconds() {
        return nakadiSettings.getDefaults().getEventsStreamTimeoutSeconds();
    }

    public Integer getEventsBatchTimeoutSeconds() {
        return nakadiSettings.getDefaults().getEventsBatchTimeoutSeconds();
    }

    public Integer getEventsBatchTimeoutSeconds(final String consumer) {
        final NakadiConsumerSettings consumerProperties = nakadiSettings.getConsumers().get(consumer);
        if (consumerProperties != null) {
            return firstNonNull(consumerProperties.getEventsBatchTimeoutSeconds(),
                    nakadiSettings.getDefaults().getEventsBatchTimeoutSeconds());
        } else {
            return nakadiSettings.getDefaults().getEventsBatchTimeoutSeconds();
        }
    }

    public Integer getEventsStreamLimit() {
        return nakadiSettings.getDefaults().getEventsStreamLimit();
    }

    public Integer getEventsStreamKeepAliveLimit() {
        return nakadiSettings.getDefaults().getEventsStreamKeepAliveLimit();
    }

    public Integer getEventsBatchLimit() {
        return nakadiSettings.getDefaults().getEventsBatchLimit();
    }

    public Integer getEventsBatchLimit(final String consumer) {
        final NakadiConsumerSettings consumerProperties = nakadiSettings.getConsumers().get(consumer);
        if (consumerProperties != null) {
            return firstNonNull(consumerProperties.getEventsBatchLimit(),
                    nakadiSettings.getDefaults().getEventsBatchLimit());
        } else {
            return nakadiSettings.getDefaults().getEventsBatchLimit();
        }
    }

}
