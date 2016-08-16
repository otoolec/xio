package com.xjeffrose.xio.client.loadbalancer.connection;

import com.xjeffrose.xio.client.XioConnectionPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class NodeConnectionPoolTest {

  @Mock
  private Future<Channel> futureChannel;

  @Mock
  private XioConnectionPool xioConnectionPool;

  @Captor
  private ArgumentCaptor<FutureListener<Channel>> futureListenerChannelCaptor;

  private Bootstrap bootstrap;
  private NodeConnectionPool nodeConnectionPool;

  @Before
  public void setUp() {
    bootstrap = new Bootstrap();
    nodeConnectionPool = new NodeConnectionPool(xioConnectionPool);
  }

  @Test
  public void testAcquireNode() throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel();

    bootstrap.group(channel.eventLoop());

    when(xioConnectionPool.acquire()).thenReturn(futureChannel);

    when(futureChannel.isSuccess()).thenReturn(true);
    when(futureChannel.getNow()).thenReturn(channel);

    Promise<NodeConnection> promise = channel.eventLoop().newPromise();
    Future<NodeConnection> connectionFuture = nodeConnectionPool.acquireConnection(promise);

    verify(futureChannel).addListener(futureListenerChannelCaptor.capture());
    futureListenerChannelCaptor.getValue().operationComplete(futureChannel);

    assertTrue(connectionFuture.isSuccess());
    assertNotNull(connectionFuture.getNow());
  }
}
