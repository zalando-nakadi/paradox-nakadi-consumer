package de.zalando.paradox.nakadi.consumer.boot;

import static java.lang.String.format;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.lang.reflect.Method;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.core.annotation.AnnotationUtils;

import org.springframework.security.oauth2.client.token.AccessTokenProvider;

import org.springframework.util.ReflectionUtils;

import org.zalando.stups.oauth2.spring.client.StupsTokensAccessTokenProvider;
import org.zalando.stups.tokens.AccessTokens;
import org.zalando.stups.tokens.config.AccessTokensBeanAutoConfiguration;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;

import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerEventConfig;
import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerEventConfigList;
import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerPartitionCoordinatorProvider;
import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;
import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.impl.SimplePartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ZKHolder;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ZKLeaderConsumerPartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ZKSimpleConsumerPartitionCoordinator;

@Configuration
@AutoConfigureAfter(AccessTokensBeanAutoConfiguration.class)
public class NakadiConsumerConfiguration {

    private static final String DEFAULT_PROPERTIES_PREFIX = "paradox.nakadi.defaults";

    private static final Logger LOGGER = LoggerFactory.getLogger(NakadiConsumerConfiguration.class);

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private NakadiSettings nakadiConsumerProperties;

    @Bean
    public ConsumerEventConfigList consumerEventConfigList() {
        final Map<String, EventHandler> handlers = applicationContext.getBeansOfType(EventHandler.class);
        final Function<Map.Entry<String, EventHandler>, List<ConsumerEventConfig>> eventHandlerFunction =
            beanNameHandlerEntry -> {
            NakadiHandler nakadiHandler = null;
            final Class<?> beanType = beanNameHandlerEntry.getValue().getClass();
            final String beanName = beanNameHandlerEntry.getKey();
            final Method method = ReflectionUtils.findMethod(beanType, "onEvent", EventTypeCursor.class, Object.class);
            if (null != method) {
                nakadiHandler = AnnotationUtils.findAnnotation(method, NakadiHandler.class);
            }

            if (null == nakadiHandler) {
                nakadiHandler = AnnotationUtils.findAnnotation(beanType, NakadiHandler.class);
            }

            final SetMultimap<String, String> consumerToEvents = LinkedHashMultimap.create();
            if (null != nakadiHandler) {
                addEventToConsumer(consumerToEvents, nakadiHandler.eventName(), nakadiHandler.consumerName(),
                    nakadiHandler.consumerNamePostfix());
            }

            if (NakadiEventHandler.class.isAssignableFrom(beanType)) {
                final Object bean = applicationContext.getBean(beanName);
                checkState(bean instanceof NakadiEventHandler, "bean must implement NakadiEventHandler");

                final NakadiEventConsumers nakadiEventConsumers = ((NakadiEventHandler) bean).getNakadiEventConsumers();
                checkArgument(null != nakadiEventConsumers, "nakadiEventConsumers must not be null");

                final Set<NakadiEventConsumer> beanEventConsumers = nakadiEventConsumers.getEventConsumers();
                if (null == beanEventConsumers || beanEventConsumers.isEmpty()) {
                    LOGGER.info("Empty Nakadi event consumers provided by [{} / {}]", beanName, beanType);
                } else {
                    beanEventConsumers.forEach(eventConsumer ->
                            addEventToConsumer(consumerToEvents, eventConsumer.getEventName(),
                                eventConsumer.getConsumerName(), false));
                }
            }

            return consumerToEvents.entries().stream().map(consumerEventEntry ->
                                           new ConsumerEventConfig(consumerEventEntry.getKey(),
                                               consumerEventEntry.getValue(), beanNameHandlerEntry.getValue())).collect(
                                       Collectors.toList());
        };

        final List<ConsumerEventConfig> list = handlers.entrySet().stream().map(eventHandlerFunction)
                                                       .flatMap(Collection::stream).collect(Collectors.toList());
        return new ConsumerEventConfigList(list);
    }

    private void addEventToConsumer(final SetMultimap<String, String> consumerToEvents, final String eventName,
            final String consumerName, final boolean consumerNamePostfix) {
        checkArgument(isNotEmpty(eventName), "eventName must not be empty");

        final String eventConsumerName;
        if (consumerNamePostfix) {
            checkArgument(isNotEmpty(consumerName), "consumerName for postfix 'true' attribute must not be empty");
            checkArgument(isNotEmpty(nakadiConsumerProperties.getDefaults().getDefaultConsumerName()),
                "defaultConsumerName for postfix 'true' attribute must not be empty");
            eventConsumerName = nakadiConsumerProperties.getDefaults().getDefaultConsumerName() + "-" + consumerName;
        } else {
            eventConsumerName = isNotEmpty(consumerName)
                ? consumerName : nakadiConsumerProperties.getDefaults().getDefaultConsumerName();
        }

        checkArgument(isNotEmpty(eventConsumerName), "consumerName must not be empty");
        consumerToEvents.put(eventConsumerName, eventName);
    }

    @Bean
    @ConditionalOnProperty(
        value = "partitionCoordinatorProvider", prefix = DEFAULT_PROPERTIES_PREFIX, matchIfMissing = true,
        havingValue = "simple"
    )
    public ConsumerPartitionCoordinatorProvider simplePartitionCoordinatorProvider(
            final Optional<List<EventErrorHandler>> eventErrorHandlerList) {
        return
            consumerName -> {
            final SimplePartitionCoordinator coordinator = new SimplePartitionCoordinator(eventErrorHandlerList.orElse(
                        Collections.emptyList()));

            // use false only for development as messages will be replayed on each restart
            coordinator.setStartNewestAvailableOffset(nakadiConsumerProperties.getDefaults()
                    .isStartNewestAvailableOffset());

            return coordinator;
        };
    }

    @Bean
    @ConditionalOnProperty(
        value = "partitionCoordinatorProvider", prefix = DEFAULT_PROPERTIES_PREFIX, havingValue = "zk"
    )
    public ConsumerPartitionCoordinatorProvider leaderConsumerPartitionCoordinator(final ZKHolder zkHolder,
            final Optional<List<EventErrorHandler>> eventErrorHandlerList) {
        return
            consumerName -> {
            final ZKLeaderConsumerPartitionCoordinator coordinator = new ZKLeaderConsumerPartitionCoordinator(zkHolder,
                    consumerName, eventErrorHandlerList.orElse(Collections.emptyList()));

            coordinator.setStartNewestAvailableOffset(nakadiConsumerProperties.getDefaults()
                    .isStartNewestAvailableOffset());
            coordinator.setDeleteUnavailableCursors(nakadiConsumerProperties.getDefaults()
                    .isDeleteUnavailableCursors());

            return coordinator;
        };
    }

    @Bean(initMethod = "init")
    @ConditionalOnProperty(
        value = "partitionCoordinatorProvider", prefix = DEFAULT_PROPERTIES_PREFIX, havingValue = "zk"
    )
    public ZKHolder zkHolder() {
        return new ZKHolder(nakadiConsumerProperties.getDefaults().getZookeeperBrokers(),
                nakadiConsumerProperties.getDefaults().getExhibitorAddresses(),
                nakadiConsumerProperties.getDefaults().getExhibitorPort());
    }

    @Bean
    @ConditionalOnProperty(
        value = "partitionCoordinatorProvider", prefix = DEFAULT_PROPERTIES_PREFIX, havingValue = "zk-simple"
    )
    public ConsumerPartitionCoordinatorProvider simpleConsumerPartitionCoordinator(final ZKHolder zkHolder,
            final Optional<List<EventErrorHandler>> eventErrorHandlerList) {
        return
            consumerName -> {
            final ZKSimpleConsumerPartitionCoordinator coordinator = new ZKSimpleConsumerPartitionCoordinator(zkHolder,
                    consumerName, eventErrorHandlerList.orElse(Collections.emptyList()));

            coordinator.setStartNewestAvailableOffset(nakadiConsumerProperties.getDefaults()
                    .isStartNewestAvailableOffset());
            coordinator.setDeleteUnavailableCursors(nakadiConsumerProperties.getDefaults()
                    .isDeleteUnavailableCursors());

            return coordinator;
        };
    }

    @Bean(initMethod = "init")
    @ConditionalOnProperty(
        value = "partitionCoordinatorProvider", prefix = DEFAULT_PROPERTIES_PREFIX, havingValue = "zk-simple"
    )
    public ZKHolder simpleZKHolder() {
        return new ZKHolder(nakadiConsumerProperties.getDefaults().getZookeeperBrokers(),
                nakadiConsumerProperties.getDefaults().getExhibitorAddresses(),
                nakadiConsumerProperties.getDefaults().getExhibitorPort());
    }

    @Bean
    @ConditionalOnProperty(
        value = "oauth2Enabled", prefix = DEFAULT_PROPERTIES_PREFIX, matchIfMissing = true, havingValue = "true"
    )
    public AuthorizationValueProvider oauth2AccessTokenProvider(final AccessTokens accessTokens) {
        final String tokenId = nakadiConsumerProperties.getDefaults().getNakadiTokenId();
        final AccessTokenProvider accessTokenProvider = new StupsTokensAccessTokenProvider(tokenId, accessTokens);
        return
            () -> {
            final String accessToken = accessTokenProvider.obtainAccessToken(null, null).getValue();
            return format("Bearer %s", accessToken);
        };
    }
}
