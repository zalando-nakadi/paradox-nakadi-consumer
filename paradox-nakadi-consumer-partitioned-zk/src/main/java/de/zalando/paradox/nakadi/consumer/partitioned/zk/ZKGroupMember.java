package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.utils.ZKPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Based on org.apache.curator:curator-recipes:2.9.1 behaviour.
 */
public class ZKGroupMember extends GroupMember {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKGroupMember.class);
    private PathChildrenCacheListener listener;
    private PathChildrenCache cache;

    // default payload - CuratorFrameworkFactory.getLocalAddress()
    public ZKGroupMember(final CuratorFramework client, final String membershipPath, final String thisId) {
        super(client, membershipPath, thisId);
    }

    public ZKGroupMember(final CuratorFramework client, final String membershipPath, final String thisId,
            final byte[] payload) {
        super(client, membershipPath, thisId, payload);
    }

    public ZKGroupMember(final CuratorFramework client, final String membershipPath, final String thisId,
            final byte[] payload, final PathChildrenCacheListener listener) {
        super(client, membershipPath, thisId, payload);
        this.listener = listener;
    }

    @Override
    public void start() {
        super.start();
        Preconditions.checkState(null != cache,
            "Implementation changed. Cache should be set - check super GroupMember");

        final PathChildrenCacheListener cacheListener = null != listener ? listener : getDefaultListener();
        cache.getListenable().addListener(cacheListener);

    }

    @Override
    protected PathChildrenCache newPathChildrenCache(final CuratorFramework client, final String membershipPath) {
        Preconditions.checkState(null == cache,
            "Implementation changed. Cache shouldn't be set - check super GroupMember");
        this.cache = super.newPathChildrenCache(client, membershipPath);
        return this.cache;
    }

    private static PathChildrenCacheListener getDefaultListener() {
        return
            (client, event) -> {
            switch (event.getType()) {

                case CHILD_ADDED : {
                    LOGGER.info("Node added: [{}]", ZKPaths.getNodeFromPath(event.getData().getPath()));
                    break;
                }

                case CHILD_REMOVED : {
                    LOGGER.info("Node removed: [{}]", ZKPaths.getNodeFromPath(event.getData().getPath()));
                    break;
                }

                default :
                    break;
            }
        };
    }
}
