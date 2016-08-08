package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.RandomUtils;

import org.junit.Test;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

public class ZKConsumerOffsetTest extends AbstractZKTest {

    @Test
    public void testManageOffset() throws Exception {
        final ZKConsumerOffset consumerOffset = new ZKConsumerOffset(zkHolder, "junit-consumer");
        final EventTypePartition etp = EventTypePartition.of(EventType.of("junit-event"), "0");

        // check null
        assertThat(consumerOffset.getOffset(etp.getEventType(), etp.getPartition())).isNull();

        // set and get
        final String offset = Long.toString(RandomUtils.nextLong(1, 1000000));
        final EventTypeCursor cursor = EventTypeCursor.of(etp, offset);
        consumerOffset.setOffset(cursor);
        assertThat(consumerOffset.getOffset(etp.getEventType(), etp.getPartition())).isEqualTo(offset);

        // delete and get
        final String path = consumerOffset.getOffsetPath(etp.getName(), etp.getPartition());
        consumerOffset.delOffset(path);
        assertThat(consumerOffset.getOffset(etp.getEventType(), etp.getPartition())).isNull();

    }
}
