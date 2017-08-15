package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.concurrent.GuardedBy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.ThreadUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

abstract class ZKConsumerLeader<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKConsumerLeader.class);

    private final ZKMember member;

    private final ZKHolder zkHolder;

    private final String consumerName;

    @GuardedBy("this")
    private final Map<T, LeaderControl> keyToLeaderControl = new HashMap<>();

    private final ThreadFactory leaderSelectorThreadFactory;

    ZKConsumerLeader(final ZKHolder zkHolder, final String consumerName, final ZKMember member) {
        this.zkHolder = requireNonNull(zkHolder, "zkHolder must not be null");
        this.consumerName = requireNonNull(consumerName, "consumerName must not be null");
        this.member = requireNonNull(member, "member must not be null");

        this.leaderSelectorThreadFactory = ThreadUtils.newThreadFactory("LeaderSelector-" + consumerName);
    }

    public abstract String getLeaderSelectorPath(final T t);

    public abstract String getLeaderInfoPath(final T t);

    synchronized void initGroupLeadership(final T t, final LeadershipChangedListener<T> leadershipChangedListener)
        throws Exception {

        if (keyToLeaderControl.containsKey(t)) {
            return;
        }

        final LeaderSelector selector = new LeaderSelector(zkHolder.getCurator(), getLeaderSelectorPath(t),
                new CloseableExecutorService(Executors.newSingleThreadExecutor(leaderSelectorThreadFactory), true),
                new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(final CuratorFramework client) throws Exception {
                        LOGGER.info("Member [{}] took leadership for [{}]", member.getMemberId(), t);
                        try {
                            final LeaderControl leaderControl = keyToLeaderControl.get(t);
                            Preconditions.checkState(leaderControl != null && !leaderControl.isLeader(),
                                "Leader control for [%s] in incorrect state", t);

                            // mark in zookeeper / information only
                            setLeaderInfo(getLeaderInfoPath(t), member.getMemberId());

                            leadershipChangedListener.takeLeadership(t, member);
                            leaderControl.takeLeadership();
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

        final LeaderControl leaderControl = LeaderControl.of(selector);
        keyToLeaderControl.put(t, leaderControl);

        LOGGER.info("Init member [{}] leadership for [{}]", member.getMemberId(), t);

        // restart selector every time the leadership is lost
        selector.autoRequeue();

        try {
            selector.start();
        } catch (final Throwable throwable) {
            keyToLeaderControl.remove(t);
            leaderControl.relinquishLeadership();
            ThrowableUtils.throwException(throwable);
        }
    }

    interface LeadershipChangedListener<T> {
        void takeLeadership(final T t, final ZKMember member);

        void relinquishLeadership(final T t, final ZKMember member);
    }

    public synchronized void closeGroupLeadership(final T t) {
        final LeaderControl leaderControl = keyToLeaderControl.remove(t);
        if (null != leaderControl) {
            LOGGER.info("Close member [{}] leadership for [{}]", member.getMemberId(), t);
            leaderControl.relinquishLeadership();
        } else {
            LOGGER.trace("Could not close member [{}] leadership for [{}] because LeaderSelector was not found",
                member.getMemberId(), t);
        }
    }

    public synchronized void close() {
        LOGGER.info("Closing for member [{}]", member.getMemberId());
        keyToLeaderControl.values().forEach(leaderControl -> {
            try {
                leaderControl.relinquishLeadership();
            } catch (Exception e) {
                LOGGER.warn("Unexpected error while closing leader selector", e);
            }
        });
        keyToLeaderControl.clear();
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

    private static class LeaderControl {

        private final LeaderSelector selector;

        private final CountDownLatch stop = new CountDownLatch(1);

        private volatile boolean leader;

        private LeaderControl(final LeaderSelector selector) {
            this.selector = selector;
        }

        static LeaderControl of(final LeaderSelector selector) {
            return new LeaderControl(selector);
        }

        /**
         * This method blocks until leadership is released by calling {@link #relinquishLeadership()}. This is needed to
         * fulfill the contract of {@link LeaderSelectorListener#takeLeadership(CuratorFramework)}.
         *
         * @throws  InterruptedException
         */
        void takeLeadership() throws InterruptedException {
            this.leader = true;
            this.stop.await();
        }

        void relinquishLeadership() {
            this.leader = false;

            // attempt to stop the LeaderSelectorListener by interrupt first then count down the latch as a safety
            // guard in case the interrupt flag is lost before calling leaderControl.takeLeadership()
            this.selector.close();
            this.stop.countDown();
        }

        boolean isLeader() {
            return leader;
        }

    }

}
