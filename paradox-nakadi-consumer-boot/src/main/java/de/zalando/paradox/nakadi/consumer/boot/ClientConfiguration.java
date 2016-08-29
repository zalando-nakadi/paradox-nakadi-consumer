package de.zalando.paradox.nakadi.consumer.boot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.springframework.boot.context.properties.EnableConfigurationProperties;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;
import de.zalando.paradox.nakadi.consumer.core.client.Client;
import de.zalando.paradox.nakadi.consumer.core.client.impl.ClientImpl;

@Configuration
@EnableConfigurationProperties(NakadiConsumerProperties.class)
public class ClientConfiguration {
    @Autowired
    private NakadiConsumerProperties nakadiConsumerProperties;

    @Autowired(required = false)
    private AuthorizationValueProvider authorizationValueProvider;

    @Autowired(required = false)
    @Qualifier("nakadiObjectMapper")
    private ObjectMapper objectMapper;

    @Bean
    public Client nakadiClient(final EventReceiverRegistryConfiguration eventReceiverConfig) {
        final ClientImpl.Builder builder = new ClientImpl.Builder(nakadiConsumerProperties.getNakadiUrl());
        if (null != authorizationValueProvider) {
            builder.withAuthorization(authorizationValueProvider);
        }

        if (null != objectMapper) {
            builder.withObjectMapper(objectMapper);
        }

        return builder.build();
    }
}
