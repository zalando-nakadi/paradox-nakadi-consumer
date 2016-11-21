package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.Collections;

import org.assertj.core.api.Assertions;

import org.junit.Test;

import org.mockito.Mockito;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.exceptions.UnrecoverableException;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallbackProvider;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListenerProvider;

public class ZKConsumerSyncOffsetManagementTest {

    @Test
    public void testShouldNotThrowExceptionWhenThereIsNoHandler() {

        new ZKConsumerSyncOffsetManagement(Mockito.mock(ZKConsumerOffset.class),
            Mockito.mock(PartitionCommitCallbackProvider.class), Mockito.mock(PartitionRebalanceListenerProvider.class),
            Collections.emptyList()).error(new IllegalStateException(), Mockito.mock(EventTypePartition.class), "2324",
            "event");
    }

    @Test
    public void testShouldThrowExceptionWhenItGetsTheUnrecoverableException() {
        final ZKConsumerSyncOffsetManagement zkConsumerSyncOffsetManagement = new ZKConsumerSyncOffsetManagement(Mockito
                    .mock(ZKConsumerOffset.class), Mockito.mock(PartitionCommitCallbackProvider.class),
                Mockito.mock(PartitionRebalanceListenerProvider.class), Collections.emptyList());

        Assertions.assertThatThrownBy(() ->
                          zkConsumerSyncOffsetManagement.error(new UnrecoverableException(),
                              Mockito.mock(EventTypePartition.class), "2324", "event")).isInstanceOf(
                      UnrecoverableException.class);
    }

    @Test
    public void testShouldLogTheExceptionWithHandler() {

        final EventErrorHandler spy = Mockito.spy(EventErrorHandler.class);

        final ZKConsumerSyncOffsetManagement zkConsumerSyncOffsetManagement = new ZKConsumerSyncOffsetManagement(Mockito
                    .mock(ZKConsumerOffset.class), Mockito.mock(PartitionCommitCallbackProvider.class),
                Mockito.mock(PartitionRebalanceListenerProvider.class), Collections.singletonList(spy));

        zkConsumerSyncOffsetManagement.error(new RuntimeException(), Mockito.mock(EventTypePartition.class), "2324",
            "event");

        Mockito.verify(spy).onError(Mockito.any(Throwable.class), Mockito.any(EventTypePartition.class),
            Mockito.anyString(), Mockito.anyString());
    }
}
