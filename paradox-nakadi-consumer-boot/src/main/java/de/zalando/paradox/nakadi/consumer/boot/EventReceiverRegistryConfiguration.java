package de.zalando.paradox.nakadi.consumer.boot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.springframework.boot.context.properties.EnableConfigurationProperties;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerEventConfigList;
import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerPartitionCoordinatorProvider;
import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;
import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;

@Configuration
@EnableConfigurationProperties(NakadiConsumerProperties.class)
public class EventReceiverRegistryConfiguration {

    @Autowired
    private NakadiConsumerProperties nakadiConsumerProperties;

    @Autowired
    private ConsumerPartitionCoordinatorProvider coordinatorProvider;

    @Autowired
    private ConsumerEventConfigList consumerEventConfigList;

    @Autowired(required = false)
    private AuthorizationValueProvider authorizationValueProvider;

    @Autowired(required = false)
    @Qualifier("nakadiObjectMapper")
    private ObjectMapper objectMapper;

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
        return nakadiConsumerProperties.getNakadiUrl();
    }

    public boolean isEventTypePartitionCoordinator() {
        return nakadiConsumerProperties.isEventTypePartitionCoordinator();
    }

    public Long getPartitionsRetryAfterMillis() {
        return nakadiConsumerProperties.getPartitionsRetryAfterMillis();
    }

    public Long getPartitionsRetryRandomMillis() {
        return nakadiConsumerProperties.getPartitionsRetryRandomMillis();
    }

    public Long getPartitionsTimeoutMillis() {
        return nakadiConsumerProperties.getPartitionsTimeoutMillis();
    }

    public Long getEventsTimeoutMillis() {
        return nakadiConsumerProperties.getEventsTimeoutMillis();
    }

    public Long getEventsRetryAfterMillis() {
        return nakadiConsumerProperties.getEventsRetryAfterMillis();
    }

    public Long getEventsRetryRandomMillis() {
        return nakadiConsumerProperties.getEventsRetryRandomMillis();
    }

    public Integer getEventsStreamTimeoutSeconds() {
        return nakadiConsumerProperties.getEventsStreamTimeoutSeconds();
    }

    public Integer getEventsBatchTimeoutSeconds() {
        return nakadiConsumerProperties.getEventsBatchTimeoutSeconds();
    }

    public Integer getEventsStreamLimit() {
        return nakadiConsumerProperties.getEventsStreamLimit();
    }

    public Integer getEventsStreamKeepAliveLimit() {
        return nakadiConsumerProperties.getEventsStreamKeepAliveLimit();
    }

    public Integer getEventsBatchLimit() {
        return nakadiConsumerProperties.getEventsBatchLimit();
    }

    public Boolean getStartNewestAvailableOffset() {
        return nakadiConsumerProperties.getStartNewestAvailableOffset();
    }

    @Bean
    public EventReceiverRegistry eventReceiverRegistry(final EventReceiverRegistryConfiguration eventReceiverConfig) {
        return new EventReceiverRegistry(eventReceiverConfig, objectMapper);
    }

}
