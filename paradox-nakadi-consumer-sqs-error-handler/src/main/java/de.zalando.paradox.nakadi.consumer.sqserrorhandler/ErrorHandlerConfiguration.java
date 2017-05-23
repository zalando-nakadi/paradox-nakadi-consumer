package de.zalando.paradox.nakadi.consumer.sqserrorhandler;

import static de.zalando.paradox.nakadi.consumer.sqserrorhandler.SQSConfiguration.DEFAULT_SQS_PROPERTIES_PREFIX;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

@ConditionalOnClass(AmazonSQS.class)
@ConditionalOnProperty(value = "enabled", prefix = DEFAULT_SQS_PROPERTIES_PREFIX, havingValue = "true")
@Configuration
public class ErrorHandlerConfiguration {

    @Bean
    public AmazonSQS amazonSQS(final SQSConfiguration sqsConfiguration) {
        final AmazonSQSClientBuilder amazonSQSClientBuilder = AmazonSQSClientBuilder.standard();
        amazonSQSClientBuilder.setCredentials(new ProfileCredentialsProvider());
        amazonSQSClientBuilder.setRegion(sqsConfiguration.getRegion());
        return amazonSQSClientBuilder.build();
    }

    @ConditionalOnMissingBean
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public SQSErrorHandler sqsErrorHandler(final AmazonSQS amazonSQS, final SQSConfiguration sqsConfiguration,
            final ObjectMapper objectMapper) {
        return new SQSErrorHandler(sqsConfiguration, amazonSQS, objectMapper);
    }
}
