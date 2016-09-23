package de.zalando.paradox.nakadi.consumer.boot;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerEventConfig;
import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerEventConfigList;
import de.zalando.paradox.nakadi.consumer.boot.components.ConsumerPartitionCoordinatorProvider;
import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;
import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.partitioned.impl.SimplePartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ZKHolder;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ZKLeaderConsumerPartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.partitioned.zk.ZKSimpleConsumerPartitionCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.security.oauth2.client.token.AccessTokenProvider;
import org.springframework.util.ReflectionUtils;
import org.zalando.stups.oauth2.spring.client.StupsTokensAccessTokenProvider;
import org.zalando.stups.tokens.AccessTokens;
import org.zalando.stups.tokens.config.AccessTokensBeanAutoConfiguration;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static de.zalando.paradox.nakadi.consumer.boot.NakadiConsumerProperties.PREFIX;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

@Configuration
@AutoConfigureAfter(AccessTokensBeanAutoConfiguration.class)
@EnableConfigurationProperties(NakadiConsumerProperties.class)
public class NakadiConsumerConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(NakadiConsumerConfiguration.class);

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private NakadiConsumerProperties nakadiConsumerProperties;

    @Bean
    public ConsumerEventConfigList consumerEventConfigList() {

        final Map<String, EventHandler> handlers = applicationContext.getBeansOfType(EventHandler.class);
        final List<ConsumerEventConfig> list = handlers.entrySet().stream().map(entry -> {
            //J-
            NakadiHandler nakadiHandler = null;
            final Class<?> beanType = entry.getValue().getClass();
            final String beanName = entry.getKey();
            final Method method = ReflectionUtils.findMethod(beanType, "onEvent", EventTypeCursor.class, Object.class);
            if (null != method) {
                nakadiHandler = AnnotationUtils.findAnnotation(method, NakadiHandler.class);
            }
            if (null == nakadiHandler) {
                nakadiHandler = AnnotationUtils.findAnnotation(beanType, NakadiHandler.class);
            }
            final SetMultimap<String, String> consumerToEvents = LinkedHashMultimap.create();
            if (null != nakadiHandler) {
                addEventToConsumer(consumerToEvents, nakadiHandler.eventName(), nakadiHandler.consumerName(), nakadiHandler.consumerNamePostfix());
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
                    beanEventConsumers.forEach(eventConsumer -> addEventToConsumer(
                            consumerToEvents, eventConsumer.getEventName(), eventConsumer.getConsumerName(), false));
                }
            }
            return consumerToEvents.entries().stream().map( element -> new ConsumerEventConfig(
                            element.getKey(), element.getValue(), entry.getValue())).
                    collect(Collectors.toList());
            //J+
        }).flatMap(Collection::stream).filter(Objects::nonNull).collect(Collectors.toList());
        return new ConsumerEventConfigList(list);
    }

    private void addEventToConsumer(final SetMultimap<String, String> consumerToEvents, final String eventName, final String consumerName,
                                    final boolean consumerNamePostfix) {
        checkArgument(isNotEmpty(eventName), "eventName must not be empty");

        final String eventConsumerName;
        if (consumerNamePostfix) {
            checkArgument(isNotEmpty(consumerName), "consumerName for postfix 'true' attribute must not be empty");
            checkArgument(isNotEmpty(nakadiConsumerProperties.getDefaultConsumerName()), "defaultConsumerName for postfix 'true' attribute must not be empty");
            eventConsumerName = nakadiConsumerProperties.getDefaultConsumerName() + "-" + consumerName;
        } else {
            eventConsumerName = isNotEmpty(consumerName) ? consumerName : nakadiConsumerProperties.getDefaultConsumerName();
        }
        checkArgument(isNotEmpty(eventConsumerName), "consumerName must not be empty");
        consumerToEvents.put(eventConsumerName, eventName);
    }

    @Bean
    @ConditionalOnProperty(value = "partitionCoordinatorProvider", prefix = PREFIX, matchIfMissing = true, havingValue = "simple")
    public ConsumerPartitionCoordinatorProvider simplePartitionCoordinatorProvider() {
        return consumerName -> {
            final SimplePartitionCoordinator coordinator = new SimplePartitionCoordinator();
            final Boolean value = nakadiConsumerProperties.getStartNewestAvailableOffset();
            if (null != value) {
                // use false only for development as messages will be replayed on each restart
                coordinator.setStartNewestAvailableOffset(value);
            }
            return coordinator;
        };
    }

    @Bean
    @ConditionalOnProperty(value = "partitionCoordinatorProvider", prefix = PREFIX, havingValue = "zk")
    public ConsumerPartitionCoordinatorProvider leaderConsumerPartitionCoordinator(final ZKHolder zkHolder) {
        return consumerName -> {
            final ZKLeaderConsumerPartitionCoordinator coordinator = new ZKLeaderConsumerPartitionCoordinator(zkHolder, consumerName);
            final Boolean value = nakadiConsumerProperties.getStartNewestAvailableOffset();
            if (null != value) {
                coordinator.setStartNewestAvailableOffset(value);
            }
            return coordinator;
        };
    }

    @Bean(initMethod = "init")
    @ConditionalOnProperty(value = "partitionCoordinatorProvider", prefix = PREFIX, havingValue = "zk")
    public ZKHolder zkHolder() {
        return new ZKHolder(nakadiConsumerProperties.getZookeeperBrokers(), nakadiConsumerProperties.getExhibitorAddresses(),
                nakadiConsumerProperties.getExhibitorPort());
    }

    @Bean
    @ConditionalOnProperty(value = "partitionCoordinatorProvider", prefix = PREFIX, havingValue = "zk-simple")
    public ConsumerPartitionCoordinatorProvider simpleConsumerPartitionCoordinator(final ZKHolder zkHolder) {
        return consumerName -> {
            final ZKSimpleConsumerPartitionCoordinator coordinator = new ZKSimpleConsumerPartitionCoordinator(zkHolder, consumerName);
            final Boolean value = nakadiConsumerProperties.getStartNewestAvailableOffset();
            if (null != value) {
                coordinator.setStartNewestAvailableOffset(value);
            }
            return coordinator;
        };
    }

    @Bean(initMethod = "init")
    @ConditionalOnProperty(value = "partitionCoordinatorProvider", prefix = PREFIX, havingValue = "zk-simple")
    public ZKHolder simpleZKHolder() {
        return new ZKHolder(nakadiConsumerProperties.getZookeeperBrokers(), nakadiConsumerProperties.getExhibitorAddresses(),
                nakadiConsumerProperties.getExhibitorPort());
    }

    @Bean
    @ConditionalOnProperty(value = "oauth2Enabled", prefix = PREFIX, matchIfMissing = true,
            havingValue = "true")
    public AuthorizationValueProvider oauth2AccessTokenProvider(AccessTokens accessTokens) {
        final String tokenId = nakadiConsumerProperties.getNakadiTokenId();
        final AccessTokenProvider accessTokenProvider = new StupsTokensAccessTokenProvider(tokenId, accessTokens);
        return () -> {
            final String accessToken = accessTokenProvider.obtainAccessToken(null, null).getValue();
            return format("Bearer %s", accessToken);
        };
    }
}
