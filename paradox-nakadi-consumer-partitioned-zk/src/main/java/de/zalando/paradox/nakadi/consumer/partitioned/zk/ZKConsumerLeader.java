package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.concurrent.GuardedBy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.ThreadUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

abstract class ZKConsumerLeader<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKConsumerLeader.class);

    private final ZKMember member;

    private final ZKHolder zkHolder;

    private final String consumerName;

    @GuardedBy("this")
    private final Map<T, LeaderSelector> leaderSelectors = new HashMap<>();

    private final ThreadFactory leaderSelectorThreadFactory;

    ZKConsumerLeader(final ZKHolder zkHolder, final String consumerName, final ZKMember member) {
        this.zkHolder = requireNonNull(zkHolder, "zkHolder must not be null");
        this.consumerName = requireNonNull(consumerName, "consumerName must not be null");
        this.member = requireNonNull(member, "member must not be null");

        this.leaderSelectorThreadFactory = ThreadUtils.newThreadFactory("LeaderSelector-" + consumerName);
    }

    interface LeadershipChangedListener<T> {
        void takeLeadership(final T t, final ZKMember member);

        void relinquishLeadership(final T t, final ZKMember member);
    }

    public abstract String getLeaderSelectorPath(final T t);

    public abstract String getLeaderInfoPath(final T t);

    synchronized void initGroupLeadership(final T t, final LeadershipChangedListener<T> leadershipChangedListener)
        throws Exception {

        if (leaderSelectors.containsKey(t)) {
            return;
        }

        final LeaderSelector selector = new LeaderSelector(zkHolder.getCurator(), getLeaderSelectorPath(t),
                new CloseableExecutorService(Executors.newSingleThreadExecutor(leaderSelectorThreadFactory), true),
                new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(final CuratorFramework client) throws Exception {
                        LOGGER.info("Member [{}] took leadership for [{}]", member.getMemberId(), t);
                        try {

                            // mark in zookeeper / information only
                            setLeaderInfo(getLeaderInfoPath(t), member.getMemberId());

                            leadershipChangedListener.takeLeadership(t, member);
                            Thread.currentThread().join();
                        } finally {
                            LOGGER.info("Member [{}] relinquished leadership for [{}]", member.getMemberId(), t);
                            leadershipChangedListener.relinquishLeadership(t, member);
                        }
                    }

                    @Override
                    public void stateChanged(final CuratorFramework client, final ConnectionState connectionState) {
                        LOGGER.info("Member [{}] connection state [{}] changed", member.getMemberId(), t);
                        super.stateChanged(client, connectionState);
                    }

                });
        selector.setId(member.getMemberId());

        LOGGER.info("Init member [{}] leadership for [{}]", member.getMemberId(), t);

        // restart selector every time the leadership is lost
        selector.autoRequeue();

        try {
            selector.start();
        } catch (final Throwable throwable) {
            leaderSelectors.remove(t);
            selector.close();
            ThrowableUtils.throwException(throwable);
        }
    }

    public synchronized void closeGroupLeadership(final T t) {
        final LeaderSelector leaderSelector = leaderSelectors.remove(t);
        if (null != leaderSelector) {
            LOGGER.info("Close member [{}] leadership for [{}]", member.getMemberId(), t);
            leaderSelector.close();
        } else {
            LOGGER.warn("Could not close member [{}] leadership for [{}] because LeaderSelector was not found",
                member.getMemberId(), t);
        }
    }

    public synchronized void close() {
        LOGGER.info("Closing for member [{}]", member.getMemberId());
        leaderSelectors.values().forEach(leaderSelector -> {
            try {
                leaderSelector.close();
            } catch (Exception e) {
                LOGGER.warn("Unexpected error while closing leader selector", e);
            }
        });
        leaderSelectors.clear();
    }

    public String getConsumerName() {
        return consumerName;
    }

    private void setLeaderInfo(final String path, final String memberId) throws Exception {
        final CuratorFramework curator = zkHolder.getCurator();
        try {
            curator.setData().forPath(path, memberId.getBytes("UTF-8"));
        } catch (KeeperException.NoNodeException e) {
            LOGGER.info("Set failed, no leader info node [{}]. Create new node", path);
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
            curator.setData().forPath(path, memberId.getBytes("UTF-8"));
        }
    }

}
