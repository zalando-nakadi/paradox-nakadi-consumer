package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.io.IOException;

import java.net.ServerSocket;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;

import org.junit.After;
import org.junit.Before;

abstract class AbstractZKTest {

    private TestingServer zkTestServer;
    ZKHolder zkHolder;

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServer(findFreePort(2181));
        zkHolder = new ZKHolder(zkTestServer.getConnectString(), "", null);
        zkTestServer.start();
        zkHolder.init();
    }

    @After
    public void tearDown() {
        try {
            if (null != zkTestServer) {
                zkTestServer.stop();
            }
        } catch (IOException ignore) { }
    }

    private static int findFreePort(final int from) {
        for (int i = from; i < 65535; i++) {
            try(ServerSocket socket = new ServerSocket(i)) {
                return socket.getLocalPort();
            } catch (java.net.BindException ignore) { }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        throw new IllegalStateException("No free port found from range:" + from);
    }

    public CuratorFramework getCurator() {
        return zkHolder.getCurator();
    }
}
