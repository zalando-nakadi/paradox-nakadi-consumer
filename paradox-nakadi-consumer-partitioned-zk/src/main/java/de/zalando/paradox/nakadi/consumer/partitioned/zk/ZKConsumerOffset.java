package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.lang.String.format;

import java.util.concurrent.CountDownLatch;

import javax.annotation.Nullable;

import org.apache.curator.framework.CuratorFramework;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

class ZKConsumerOffset {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKConsumerOffset.class);

    private static final String CONSUMER_OFFSET = "/paradox/nakadi/event_types/%s/partitions/%s/consumers/%s/offset";

    private final ZKHolder zkHolder;

    private final String consumerName;

    ZKConsumerOffset(final ZKHolder zkHolder, final String consumerName) {
        this.zkHolder = zkHolder;
        this.consumerName = consumerName;
    }

    @Nullable
    String getOffset(final EventType eventType, final String partition) throws Exception {
        final String path = getOffsetPath(eventType.getName(), partition);
        return getOffset(path);
    }

    @Nullable
    String getOffset(final EventTypePartition eventTypePartition) throws Exception {
        final String path = getOffsetPath(eventTypePartition.getName(), eventTypePartition.getPartition());
        return getOffset(path);
    }

    void setOffset(final EventTypeCursor cursor) throws Exception {
        final String path = getOffsetPath(cursor.getName(), cursor.getPartition());
        setOffset(path, cursor.getOffset());
    }

    private void setOffset(final String path, final String offset) throws Exception {
        final CuratorFramework curator = zkHolder.getCurator();
        try {
            curator.setData().forPath(path, offset.getBytes("UTF-8"));
        } catch (KeeperException.NoNodeException e) {
            LOGGER.info("Set failed, no offset node [{}]. Create new node", path);
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
            curator.setData().forPath(path, offset.getBytes("UTF-8"));
        }
    }

    void delOffset(final EventTypePartition eventTypePartition) throws Exception {
        final String path = getOffsetPath(eventTypePartition.getName(), eventTypePartition.getPartition());
        delOffset(path);
    }

    void delOffset(final String path) throws Exception {
        try {
            zkHolder.getCurator().delete().forPath(path);
        } catch (KeeperException.NoNodeException ignored) {
            LOGGER.info("Delete failed, no offset node [{}]", path);
        }
    }

    @Nullable
    String getOffset(final String path) throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        zkHolder.getCurator().sync().inBackground((client, event) -> countDownLatch.countDown()).forPath(path);
        countDownLatch.await();

        try {
            final byte[] data = zkHolder.getCurator().getData().forPath(path);
            return null != data && data.length != 0 ? new String(data, "UTF-8") : null;
        } catch (KeeperException.NoNodeException e) {
            LOGGER.info("Get failed, no offset node [{}]", path);
            return null;
        }
    }

    String getOffsetPath(final String name, final String partition) {
        return format(CONSUMER_OFFSET, name, partition, consumerName);
    }

    String getConsumerName() {
        return consumerName;
    }
}
