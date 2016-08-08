package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;

import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.exhibitor.ExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.google.common.base.Preconditions;

public class ZKHolder {

    private CuratorFramework curator;

    private final String zookeeperBrokers;

    private final String exhibitorAddresses;

    private final Integer exhibitorPort;

    public ZKHolder(final String zookeeperBrokers, final String exhibitorAddresses, final Integer exhibitorPort) {
        this.zookeeperBrokers = zookeeperBrokers;
        this.exhibitorAddresses = exhibitorAddresses;
        this.exhibitorPort = exhibitorPort;
    }

    public void init() throws Exception {
        Preconditions.checkArgument(StringUtils.isNotEmpty(zookeeperBrokers), "zookeeperBrokers must not be empty");

        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final EnsembleProvider ensembleProvider;
        if (StringUtils.isNotEmpty(exhibitorAddresses)) {
            final Collection<String> exhibitorHosts = Arrays.asList(exhibitorAddresses.split("\\s*,\\s*"));
            final Exhibitors exhibitors = new Exhibitors(exhibitorHosts, exhibitorPort, () -> zookeeperBrokers);
            final ExhibitorRestClient exhibitorRestClient = new DefaultExhibitorRestClient();
            ensembleProvider = new ExhibitorEnsembleProvider(exhibitors, exhibitorRestClient,
                    "/exhibitor/v1/cluster/list", 300000, retryPolicy);
            ((ExhibitorEnsembleProvider) ensembleProvider).pollForInitialEnsemble();
        } else {
            ensembleProvider = new FixedEnsembleProvider(zookeeperBrokers);
        }

        curator = CuratorFrameworkFactory.builder().ensembleProvider(ensembleProvider).retryPolicy(retryPolicy).build();
        curator.start();
    }

    public CuratorFramework getCurator() {
        return curator;
    }
}
