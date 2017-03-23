package de.zalando.paradox.nakadi.consumer.boot;

import org.springframework.boot.bind.PropertiesConfigurationFactory;

import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;

@Configuration
public class NakadiSettingsConfiguration implements EnvironmentAware {

    private ConfigurableEnvironment environment;

    @Bean
    public NakadiSettings nakadiSettings() {
        final PropertiesConfigurationFactory<NakadiSettings> propertiesConfigurationFactory =
            new PropertiesConfigurationFactory<>(NakadiSettings.class);
        propertiesConfigurationFactory.setConversionService(environment.getConversionService());
        propertiesConfigurationFactory.setPropertySources(environment.getPropertySources());
        propertiesConfigurationFactory.setTargetName("paradox.nakadi");
        try {
            return propertiesConfigurationFactory.getObject();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setEnvironment(final Environment environment) {
        this.environment = (ConfigurableEnvironment) environment;
    }

}
