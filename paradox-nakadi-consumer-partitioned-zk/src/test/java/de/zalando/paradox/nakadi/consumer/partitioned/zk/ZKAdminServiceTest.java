package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import org.junit.Before;
import org.junit.Test;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

public class ZKAdminServiceTest extends AbstractZKTest {

    private static final EventType EVENT_ORDER_RECEIVED = EventType.of("ORDER.Received");
    private static final EventType EVENT_ORDER_CANCELED = EventType.of("order_canceled");
    private static final String CONSUMER_1 = "consumer-1";
    private static final String CONSUMER_2 = "ConsumeR.2";
    private static final String CONSUMER_3 = "___very_...___strange_Name__";

    private ZKAdminService adminService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        adminService = new ZKAdminService(zkHolder);
    }

    @Test
    public void testMemberData() throws Exception {

        ZKConsumerGroupMember groupMember1 = new ZKConsumerGroupMember(zkHolder, CONSUMER_1, ZKMember.of("member1"));
        storeMember(groupMember1, EVENT_ORDER_RECEIVED.getName());

        ZKConsumerGroupMember groupMember2 = new ZKConsumerGroupMember(zkHolder, CONSUMER_2, ZKMember.of("member2"));
        storeMember(groupMember2, EVENT_ORDER_CANCELED.getName());

        assertThat(adminService.getConsumerInfo()).hasSize(2);

        List<Map<String, Object>> map = adminService.getEventConsumerInfo(EVENT_ORDER_RECEIVED);
        assertThat(map).hasSize(1);
        assertThat(map.get(0)).contains(entry("eventName", EVENT_ORDER_RECEIVED.getName()),
            entry("consumerName", CONSUMER_1));
        assertThat(map.get(0)).containsKeys("host", "memberId", "created");

        map = adminService.getEventConsumerInfo(EVENT_ORDER_CANCELED);
        assertThat(map).hasSize(1);
        assertThat(map.get(0)).contains(entry("eventName", EVENT_ORDER_CANCELED.getName()),
            entry("consumerName", CONSUMER_2));
        assertThat(map.get(0)).containsKeys("host", "memberId", "created");

    }

    private void storeMember(final ZKConsumerGroupMember groupMember, final String eventName) throws Exception {
        final ZKMember member = groupMember.getMember();
        final String path = groupMember.getConsumerGroupPath(eventName) + "/" + member.getMemberId();
        final CuratorFramework curator = zkHolder.getCurator();
        byte[] bytes = member.toByteJson();
        try {
            curator.setData().forPath(path, bytes);
        } catch (KeeperException.NoNodeException e) {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
            curator.setData().forPath(path, bytes);
        }
    }

    @Test
    public void testEventData() {
        final EventTypePartition received0 = EventTypePartition.of(EVENT_ORDER_RECEIVED, "0");
        final EventTypePartition received1 = EventTypePartition.of(EVENT_ORDER_RECEIVED, "1");
        final EventTypePartition canceled1 = EventTypePartition.of(EVENT_ORDER_CANCELED, "1");

        //
        assertThat(adminService.getEventTypes()).isEmpty();
        assertThat(adminService.getConsumerNames()).isEmpty();
        assertThat(adminService.getEventConsumerNames(EVENT_ORDER_RECEIVED)).isEmpty();
        assertThat(adminService.getEventConsumerNames(EVENT_ORDER_CANCELED)).isEmpty();
        assertThat(adminService.getCustomerOffset(CONSUMER_1, received0)).isNull();
        assertThat(adminService.getCustomerOffset(CONSUMER_2, received0)).isNull();
        assertThat(adminService.getCustomerOffset(CONSUMER_1, received1)).isNull();
        assertThat(adminService.getCustomerOffset(CONSUMER_2, received1)).isNull();

        //
        adminService.setCustomerOffset(CONSUMER_1, EventTypeCursor.of(received0, "10001"));
        assertThat(adminService.getCustomerOffset(CONSUMER_1, received0)).isEqualTo("10001");
        assertThat(adminService.getEventPartitions(EVENT_ORDER_RECEIVED)).containsExactly(received0);
        assertThat(adminService.getEventTypes()).containsExactly(EVENT_ORDER_RECEIVED);

        assertThat(adminService.getConsumerNames()).containsExactly(CONSUMER_1);
        assertThat(adminService.getEventConsumerNames(EVENT_ORDER_RECEIVED)).containsExactly(CONSUMER_1);
        assertThat(adminService.getEventConsumerNames(EVENT_ORDER_CANCELED)).isEmpty();

        //
        adminService.setCustomerOffset(CONSUMER_2, EventTypeCursor.of(received1, "10012"));
        assertThat(adminService.getCustomerOffset(CONSUMER_2, received1)).isEqualTo("10012");
        assertThat(adminService.getEventPartitions(EVENT_ORDER_RECEIVED)).containsOnly(received0, received1);
        assertThat(adminService.getEventTypes()).containsExactly(EVENT_ORDER_RECEIVED);

        assertThat(adminService.getConsumerNames()).containsOnly(CONSUMER_1, CONSUMER_2);
        assertThat(adminService.getEventConsumerNames(EVENT_ORDER_RECEIVED)).containsOnly(CONSUMER_1, CONSUMER_2);
        assertThat(adminService.getEventConsumerNames(EVENT_ORDER_CANCELED)).isEmpty();

        //
        adminService.setCustomerOffset(CONSUMER_3, EventTypeCursor.of(canceled1, "20033"));
        assertThat(adminService.getCustomerOffset(CONSUMER_3, canceled1)).isEqualTo("20033");
        assertThat(adminService.getEventPartitions(EVENT_ORDER_CANCELED)).containsOnly(canceled1);
        assertThat(adminService.getEventTypes()).containsOnly(EVENT_ORDER_RECEIVED, EVENT_ORDER_CANCELED);

        assertThat(adminService.getConsumerNames()).containsOnly(CONSUMER_1, CONSUMER_2, CONSUMER_3);
        assertThat(adminService.getEventConsumerNames(EVENT_ORDER_RECEIVED)).containsOnly(CONSUMER_1, CONSUMER_2);
        assertThat(adminService.getEventConsumerNames(EVENT_ORDER_CANCELED)).containsOnly(CONSUMER_3);

        // delete with ignore
        adminService.delCustomerOffset(CONSUMER_1, received0);
        adminService.delCustomerOffset(CONSUMER_1, received1);
        adminService.delCustomerOffset(CONSUMER_1, canceled1);
        assertThat(adminService.getCustomerOffset(CONSUMER_1, received0)).isNull();
        assertThat(adminService.getCustomerOffset(CONSUMER_1, received1)).isNull();
        assertThat(adminService.getCustomerOffset(CONSUMER_1, canceled1)).isNull();
    }
}
