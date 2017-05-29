package de.zalando.paradox.nakadi.consumer.boot;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomUtils.nextLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import de.zalando.paradox.nakadi.consumer.boot.components.EventReceiverRegistry;
import de.zalando.paradox.nakadi.consumer.boot.components.EventTypeConsumer;
import de.zalando.paradox.nakadi.consumer.core.EventHandler;
import de.zalando.paradox.nakadi.consumer.core.FailedEventSource;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.FailedEvent;

public class FailedEventReplayerTest {

    private static final String EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE = "Event source name is not available.";

    private static final String TEST_EVENT_SOURCE_NAME = randomAlphabetic(10);

    private FailedEventReplayer failedEventReplayer;

    private EventReceiverRegistry eventReceiverRegistry;

    private FailedEventSource<FailedEvent> failedEventSource;

    private ReplayHandler replayHandler;

    @Before
    public void setUp() {

        replayHandler = mock(ReplayHandler.class);

        failedEventSource = mock(FailedEventSource.class);
        when(failedEventSource.getEventSourceName()).thenReturn(TEST_EVENT_SOURCE_NAME);
        eventReceiverRegistry = mock(EventReceiverRegistry.class);

        failedEventReplayer = new FailedEventReplayer(eventReceiverRegistry,
                Collections.singletonList(failedEventSource), replayHandler);
    }

    @Test
    public void testShouldFailWhileGettingSizeOfEventsWhenTheEventNameIsNotAvailable() {
        assertThatThrownBy(() -> failedEventReplayer.getTotalNumberOfFailedEvents(randomAlphabetic(2))).isInstanceOf(
            IllegalArgumentException.class).hasMessage(EVENT_SOURCE_NAME_IS_NOT_AVAILABLE_MESSAGE);
    }

    @Test
    public void testShouldReturnSizeOfEvents() {

        final long actualTotalNumberOfFailedEvents = nextLong(1, 10);
        when(failedEventSource.getSize()).thenReturn(actualTotalNumberOfFailedEvents);
        assertThat(failedEventReplayer.getTotalNumberOfFailedEvents(TEST_EVENT_SOURCE_NAME)).isEqualTo(
            actualTotalNumberOfFailedEvents);
    }

    @Test
    public void testShouldReturnFailedEventSourceNames() {
        assertThat(failedEventReplayer.getFailedEventSources()).containsExactly(TEST_EVENT_SOURCE_NAME);
    }

    @Test
    public void testShouldFailWhenExceptionOccurredWhileReplaying() {

        final long totalNumberOfFailedEvents = nextLong(1, 10);
        when(failedEventSource.getSize()).thenReturn(totalNumberOfFailedEvents);

        when(failedEventSource.getFailedEvent()).thenReturn(Optional.of(generateFailedEvent()));

        doThrow(RuntimeException.class).when(replayHandler).handle(anyString(), any(EventHandler.class),
            any(EventTypePartition.class), anyString());

        when(eventReceiverRegistry.getEventTypeConsumerHandler(any(EventTypeConsumer.class))).thenReturn(mock(
                EventHandler.class));

        assertThatThrownBy(() -> failedEventReplayer.replay(TEST_EVENT_SOURCE_NAME, 20L, true)).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(
                                    "Exception occurred while processing the event. Event = ");

        verify(eventReceiverRegistry).getEventTypeConsumerHandler(any(EventTypeConsumer.class));
        verify(replayHandler).handle(anyString(), any(EventHandler.class), any(EventTypePartition.class), anyString());
    }

    @Test
    public void testShouldNotFailWhenExceptionOccurredWhileReplaying() {

        final long totalNumberOfFailedEvents = nextLong(3, 10);
        when(failedEventSource.getSize()).thenReturn(totalNumberOfFailedEvents);

        when(failedEventSource.getFailedEvent()).thenReturn(Optional.of(generateFailedEvent()));

        doThrow(RuntimeException.class).when(replayHandler).handle(anyString(), any(EventHandler.class),
            any(EventTypePartition.class), anyString());

        when(eventReceiverRegistry.getEventTypeConsumerHandler(any(EventTypeConsumer.class))).thenReturn(mock(
                EventHandler.class));

        failedEventReplayer.replay(TEST_EVENT_SOURCE_NAME, 20L, false);

        verify(eventReceiverRegistry, times((int) totalNumberOfFailedEvents)).getEventTypeConsumerHandler(any(
                EventTypeConsumer.class));
        verify(replayHandler, times((int) totalNumberOfFailedEvents)).handle(anyString(), any(EventHandler.class),
            any(EventTypePartition.class), anyString());
    }

    @Test
    public void testShouldFailIfEventHandlerIsNotFound() {

        final long totalNumberOfFailedEvents = nextLong(3, 10);
        when(failedEventSource.getSize()).thenReturn(totalNumberOfFailedEvents);

        when(failedEventSource.getFailedEvent()).thenReturn(Optional.of(generateFailedEvent()));

        failedEventReplayer.replay(TEST_EVENT_SOURCE_NAME, 20L, false);

        verify(eventReceiverRegistry, times((int) totalNumberOfFailedEvents)).getEventTypeConsumerHandler(any(
                EventTypeConsumer.class));
        verify(replayHandler, never()).handle(anyString(), any(EventHandler.class), any(EventTypePartition.class),
            anyString());
    }

    @Test
    public void testShouldReplayTheFailedEvents() {

        final FailedEvent failedEvent = generateFailedEvent();

        final long totalNumberOfFailedEvents = nextLong(3, 10);
        when(failedEventSource.getSize()).thenReturn(totalNumberOfFailedEvents);

        when(failedEventSource.getFailedEvent()).thenReturn(Optional.of(failedEvent));
        when(eventReceiverRegistry.getEventTypeConsumerHandler(any(EventTypeConsumer.class))).thenReturn(mock(
                EventHandler.class));

        failedEventReplayer.replay(TEST_EVENT_SOURCE_NAME, 20L, false);

        verify(eventReceiverRegistry, times((int) totalNumberOfFailedEvents)).getEventTypeConsumerHandler(any(
                EventTypeConsumer.class));
        verify(replayHandler, times((int) totalNumberOfFailedEvents)).handle(anyString(), any(EventHandler.class),
            any(EventTypePartition.class), anyString());
    }

    @Test
    public void testShouldFailWhenExceptionOccurredDuringCommitment() {

        final FailedEvent failedEvent = generateFailedEvent();

        final long totalNumberOfFailedEvents = nextLong(3, 10);
        when(failedEventSource.getSize()).thenReturn(totalNumberOfFailedEvents);

        when(failedEventSource.getFailedEvent()).thenReturn(Optional.of(failedEvent));
        when(eventReceiverRegistry.getEventTypeConsumerHandler(any(EventTypeConsumer.class))).thenReturn(mock(
                EventHandler.class));
        doThrow(RuntimeException.class).when(failedEventSource).commit(failedEvent);

        assertThatThrownBy(() -> failedEventReplayer.replay(TEST_EVENT_SOURCE_NAME, 20L, true)).isInstanceOf(
            IllegalStateException.class).hasMessageContaining(
                                    "Exception occurred while processing the event. Event = ");

        verify(eventReceiverRegistry).getEventTypeConsumerHandler(any(EventTypeConsumer.class));
        verify(replayHandler).handle(anyString(), any(EventHandler.class), any(EventTypePartition.class), anyString());
    }

    private FailedEvent generateFailedEvent() {
        final FailedEvent failedEvent = new FailedEvent();
        failedEvent.setRawEvent(randomAlphabetic(10));
        failedEvent.setOffset(randomAlphabetic(10));
        failedEvent.setConsumerName(randomAlphabetic(10));
        failedEvent.setFailedTimeInMilliSeconds(nextLong(1, 10));
        failedEvent.setEventType(new EventType(randomAlphabetic(10)));
        failedEvent.setPartition(randomAlphabetic(10));
        failedEvent.setThrowable(new Exception(randomAlphabetic(10)));
        return failedEvent;
    }
}
