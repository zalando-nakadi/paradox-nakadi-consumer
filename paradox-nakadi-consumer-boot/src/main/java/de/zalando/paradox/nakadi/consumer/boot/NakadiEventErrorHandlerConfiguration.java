package de.zalando.paradox.nakadi.consumer.boot;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import de.zalando.paradox.nakadi.consumer.boot.components.EventErrorHandlerList;
import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;
import de.zalando.paradox.nakadi.consumer.core.FailedEventSource;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;

@Configuration
public class NakadiEventErrorHandlerConfiguration {

    @Autowired
    private ApplicationContext applicationContext;

    @Bean
    public EventErrorHandlerList eventErrorHandlerList() {

        final Map<String, EventErrorHandler> eventErrorHandlerMap = applicationContext.getBeansOfType(
                EventErrorHandler.class);

        if (null != eventErrorHandlerMap && !eventErrorHandlerMap.isEmpty()) {
            return new EventErrorHandlerList(eventErrorHandlerMap.entrySet().stream().map(Map.Entry::getValue).collect(
                        Collectors.toList()));
        } else {
            return new EventErrorHandlerList();
        }
    }

    @Bean
    public FailedEventReplayer failedEventHandler(final EventReceiverRegistry eventReceiverRegistry,
            final Optional<List<FailedEventSource>> failedEventSources) {
        return new FailedEventReplayer(eventReceiverRegistry, failedEventSources.orElse(Collections.emptyList()),
                new ReplayHandler());
    }
}
