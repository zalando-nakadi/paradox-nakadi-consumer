package de.zalando.paradox.nakadi.consumer.boot;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomNumeric;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import org.springframework.web.context.request.async.DeferredResult;

import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;
import de.zalando.paradox.nakadi.consumer.boot.components.EventTypeConsumer;
import de.zalando.paradox.nakadi.consumer.core.client.Client;

public class EventHandlerControllerTest {

    @InjectMocks
    private ControllerConfiguration.EventHandlerController eventHandlerController;

    @Mock
    private EventReceiverRegistry eventReceiverRegistry;

    @Mock
    private Client client;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testShouldValidateConsumerNameAndEventType() {
        final Set<EventTypeConsumer> eventTypeConsumerSet = new HashSet<>();
        eventTypeConsumerSet.add(new EventTypeConsumer(randomAlphabetic(10), randomAlphabetic(10)));
        eventTypeConsumerSet.add(new EventTypeConsumer(randomAlphabetic(10), randomAlphabetic(10)));

        when(eventReceiverRegistry.getEventTypeConsumers()).thenReturn(eventTypeConsumerSet);

        final DeferredResult<ResponseEntity<?>> deferredResult = eventHandlerController.replay(randomAlphabetic(10),
                randomNumeric(2), randomNumeric(2), randomAlphabetic(10), new Random().nextBoolean());

        final ResponseEntity<String> responseEntity = (ResponseEntity<String>) deferredResult.getResult();
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(responseEntity.getBody()).isEqualTo("Consumer not found.");
    }
}
