package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.Collections;

import org.assertj.core.api.Assertions;

import org.junit.Test;

import org.mockito.Mockito;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.exceptions.UnrecoverableException;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallbackProvider;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListenerProvider;

public class ZKConsumerSyncOffsetManagementTest {

    @Test
    public void testShouldNotThrowExceptionWhenThereIsNoHandler() {

        final ZKConsumerSyncOffsetManagement zkConsumerSyncOffsetManagement = new ZKConsumerSyncOffsetManagement(Mockito
                    .mock(ZKConsumerOffset.class), Mockito.mock(PartitionCommitCallbackProvider.class),
                Mockito.mock(PartitionRebalanceListenerProvider.class), Collections.emptyList());

        zkConsumerSyncOffsetManagement.error(new IllegalStateException(), Mockito.mock(EventTypePartition.class),
            "2324", "event");
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
}
