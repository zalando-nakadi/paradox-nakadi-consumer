package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Matchers.any;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Iterables;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionCommitCallbackProvider;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListener;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionRebalanceListenerProvider;

public class ZKConsumerSyncOffsetManagementInvalidCursorsTest extends AbstractZKTest {

    private static final String TEST_EVENT = "test-event";

    private static final String TEST_PARTITION = "8";

    private ZKConsumerSyncOffsetManagement zkConsumerSyncOffsetManagement;

    private ZKConsumerOffset consumerOffset;

    @Mock
    private PartitionCommitCallbackProvider mockPartitionCommitCallbackProvider;

    @Mock
    private PartitionRebalanceListenerProvider mockPartitionRebalanceListenerProvider;

    @Mock
    private PartitionRebalanceListener mockPartitionRebalanceListener;

    @Captor
    private ArgumentCaptor<Collection<EventTypePartition>> partitionsCaptor;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
        consumerOffset = new ZKConsumerOffset(zkHolder, "test-consumer");
        zkConsumerSyncOffsetManagement = new ZKConsumerSyncOffsetManagement(consumerOffset,
                mockPartitionCommitCallbackProvider, mockPartitionRebalanceListenerProvider, Collections.emptyList());
    }

    @Test
    public void testShouldDeleteInvalidCursors() throws Exception {
        when(mockPartitionRebalanceListenerProvider.getPartitionRebalanceListener(any(EventType.class))).thenReturn(
            mockPartitionRebalanceListener);

        final EventTypePartition eventTypePartition = EventTypePartition.of(EventType.of(TEST_EVENT), TEST_PARTITION);
        consumerOffset.setOffset(EventTypeCursor.of(eventTypePartition, "123"));
        zkConsumerSyncOffsetManagement.setDeleteUnavailableCursors(true);

        zkConsumerSyncOffsetManagement.error(412,
            "{\"type\":\"http://httpstatus.es/412\",\"title\":\"Precondition Failed\",\"status\":412,\"detail\":\"offset 123 for partition 8 is unavailable\"}",
            EventTypePartition.of(EventType.of(TEST_EVENT), TEST_PARTITION));

        assertThat(consumerOffset.getOffset(eventTypePartition)).isNull();

        verify(mockPartitionRebalanceListener).onPartitionsRevoked(partitionsCaptor.capture());

        final Collection<EventTypePartition> partitionsCaptorValue = partitionsCaptor.getValue();
        assertThat(partitionsCaptorValue).hasSize(1);
        assertThat(Iterables.getOnlyElement(partitionsCaptorValue).getEventType().getName()).isEqualTo(TEST_EVENT);
        assertThat(Iterables.getOnlyElement(partitionsCaptorValue).getPartition()).isEqualTo(TEST_PARTITION);
    }

    @Test
    public void testShouldNotDeleteCursorIfProvidedOffsetWasNull() throws Exception {
        final EventTypePartition eventTypePartition = EventTypePartition.of(EventType.of(TEST_EVENT), TEST_PARTITION);
        consumerOffset.setOffset(EventTypeCursor.of(eventTypePartition, "123"));
        zkConsumerSyncOffsetManagement.setDeleteUnavailableCursors(true);

        zkConsumerSyncOffsetManagement.error(412,
            "{\"type\":\"http://httpstatus.es/412\",\"title\":\"Precondition Failed\",\"status\":412,\"detail\":\"offset must not be null\"}",
            EventTypePartition.of(EventType.of(TEST_EVENT), TEST_PARTITION));

        assertThat(consumerOffset.getOffset(eventTypePartition)).isEqualTo("123");
    }

}
