package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;

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

    private final ConcurrentMap<T, LeaderSelector> leaderSelectors = new ConcurrentHashMap<>();

    ZKConsumerLeader(final ZKHolder zkHolder, final String consumerName, final ZKMember member) {
        this.zkHolder = requireNonNull(zkHolder, "zkHolder must not be null");
        this.consumerName = requireNonNull(consumerName, "consumerName must not be null");
        this.member = requireNonNull(member, "member must not be null");
    }

    interface LeadershipChangedListener<T> {
        void takeLeadership(final T t, final ZKMember member);

        void relinquishLeadership(final T t, final ZKMember member);
    }

    public abstract String getLeaderSelectorPath(final T t);

    public abstract String getLeaderInfoPath(final T t);

    void initGroupLeadership(final T t, final LeadershipChangedListener<T> leadershipChangedListener) throws Exception {

        if (leaderSelectors.containsKey(t)) {
            return;
        }

        final LeaderSelector selector = new LeaderSelector(zkHolder.getCurator(), getLeaderSelectorPath(t),
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

        if (null == leaderSelectors.putIfAbsent(t, selector)) {
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
        } else {
            selector.close();
        }
    }

    public void closeGroupLeadership(final T t) {
        final LeaderSelector leaderSelector = leaderSelectors.remove(t);
        if (null != leaderSelector) {
            LOGGER.info("Close member [{}] leadership for [{}]", member.getMemberId(), t);
            CloseableUtils.closeQuietly(leaderSelector);
        }
    }

    public void close() {
        LOGGER.info("Closing for member [{}]", member.getMemberId());
        leaderSelectors.values().forEach(CloseableUtils::closeQuietly);
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
