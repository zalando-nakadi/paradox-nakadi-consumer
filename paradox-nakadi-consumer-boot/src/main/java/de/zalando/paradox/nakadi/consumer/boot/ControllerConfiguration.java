package de.zalando.paradox.nakadi.consumer.boot;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.context.annotation.Configuration;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;

@Configuration
public class ControllerConfiguration {

    @RestController
    @RequestMapping(value = "/nakadi/event-receivers")
    public static class EventReceiverController {

        private EventReceiverRegistry registry;

        @Autowired
        public EventReceiverController(final EventReceiverRegistry registry) {
            this.registry = registry;
        }

        @RequestMapping(value = "/stop", method = RequestMethod.POST)
        public void stop() throws Exception {
            registry.stop();
        }

        @RequestMapping(value = "/restart", method = RequestMethod.POST)
        public void restart() {
            registry.restart();
        }
    }

}
