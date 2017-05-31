package de.zalando.paradox.nakadi.consumer.boot;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;
import de.zalando.paradox.nakadi.consumer.core.FailedEventSource;

@Configuration
public class NakadiEventErrorHandlerConfiguration {

    @Bean
    public FailedEventReplayer failedEventHandler(final EventReceiverRegistry eventReceiverRegistry,
            final Optional<List<FailedEventSource>> failedEventSources) {
        return new FailedEventReplayer(eventReceiverRegistry, failedEventSources.orElse(Collections.emptyList()),
                new ReplayHandler());
    }
}
