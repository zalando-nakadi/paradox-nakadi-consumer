package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static org.mockito.Matchers.eq;

import java.util.Collections;

import org.assertj.core.api.Assertions;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.exceptions.UnrecoverableException;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallbackProvider;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListenerProvider;

public class ZKConsumerSyncOffsetManagementTest {

    private ZKConsumerSyncOffsetManagement zKConsumerSyncOffsetManagement;

    @Mock
    private PartitionCommitCallbackProvider partitionCommitCallbackProvider;

    @Mock
    private ZKConsumerOffset zkConsumerOffset;

    @Mock
    private PartitionRebalanceListenerProvider partitionRebalanceListenerProvider;

    @Mock
    private EventErrorHandler eventErrorHandler;

    @Mock
    private EventTypePartition eventTypePartition;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        zKConsumerSyncOffsetManagement = new ZKConsumerSyncOffsetManagement(zkConsumerOffset,
                partitionCommitCallbackProvider, partitionRebalanceListenerProvider,
                Collections.singletonList(eventErrorHandler));
    }

    @Test
    public void testShouldThrowExceptionWhenItGetsTheUnrecoverableException() {
        Assertions.assertThatThrownBy(() ->
                          zKConsumerSyncOffsetManagement.error(new UnrecoverableException(), eventTypePartition, "2324",
                              "event")).isInstanceOf(UnrecoverableException.class);
    }

    @Test
    public void testShouldLogTheExceptionWithHandler() {

        zKConsumerSyncOffsetManagement.error(new RuntimeException(), eventTypePartition, "2324", "event");

        Mockito.verify(eventErrorHandler).onError(Mockito.any(Throwable.class), Mockito.any(EventTypePartition.class),
            eq("2324"), Mockito.anyString());
    }
}
