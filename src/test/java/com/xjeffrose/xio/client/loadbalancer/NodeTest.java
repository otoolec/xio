package com.xjeffrose.xio.client.loadbalancer;

import com.google.common.collect.ImmutableList;
import com.xjeffrose.xio.client.loadbalancer.connection.ChannelNodeConnection;
import com.xjeffrose.xio.client.loadbalancer.connection.NodeConnection;
import com.xjeffrose.xio.client.loadbalancer.connection.NodeConnectionPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NodeTest {

  @Mock
  private NodeConnectionPool connectionPool;

  @Mock
  private InetSocketAddress address;

  @Mock
  private Future<Channel> futureChannel;

  private ImmutableList<String> filters;
  private int weight;
  private String serviceName;
  private Protocol proto;
  private boolean ssl;
  private Bootstrap bootstrap;

  @Before
  public void setUp() {
    filters = ImmutableList.of();
    weight = 1;
    serviceName = "testserv";
    proto = Protocol.TCP;
    ssl = false;
    bootstrap = new Bootstrap();
  }

  @Test
  public void testSend_successfulWrite() throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelNodeConnection nodeConnection =
      createChannelNodeConnection(channel);
    Node node = createNode();
    Object message = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");

    Future<Void> sendFuture = node.send(message);

    assertEquals(true, sendFuture.isSuccess());
    assertEquals(message, channel.outboundMessages().poll());
    verify(connectionPool).releaseConnection(nodeConnection);
  }

  @Test
  public void testSend_failedWrite() throws Exception {
    final RuntimeException cause = new RuntimeException("testing error on write");
    EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
      @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
        throw cause;
      }
    });
    ChannelNodeConnection nodeConnection = createChannelNodeConnection(channel);

    Node node = createNode();
    Object message = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");

    Future<Void> sendFuture = node.send(message);

    assertEquals(false, sendFuture.isSuccess());
    assertEquals(cause, sendFuture.cause());
    assertEquals(0, channel.outboundMessages().size());
    verify(connectionPool).releaseConnection(nodeConnection);
  }

  @Test
  public void testSend_failedAcquire() throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel();
    bootstrap.group(channel.eventLoop());
    ChannelNodeConnection nodeConnection = new ChannelNodeConnection(channel);
    final Throwable cause = new Throwable("ran out of resources");
    when(connectionPool.acquireConnection(any(Promise.class))).then(invocation -> {
      Promise<NodeConnection> promise = invocation.getArgumentAt(0, Promise.class);
      promise.setFailure(cause);
      return promise;
    });

    Node node = createNode();
    Object message = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");

    Future<Void> sendFuture = node.send(message);

    assertEquals(false, sendFuture.isSuccess());
    assertEquals(cause, sendFuture.cause());
    assertEquals(0, channel.outboundMessages().size());
    verify(connectionPool, never()).releaseConnection(nodeConnection);
  }

  ChannelNodeConnection createChannelNodeConnection(EmbeddedChannel channel) {
    bootstrap.group(channel.eventLoop());
    ChannelNodeConnection nodeConnection = new ChannelNodeConnection(channel);
    when(connectionPool.acquireConnection(any(Promise.class))).then(invocation -> {
      Promise<NodeConnection> promise = invocation.getArgumentAt(0, Promise.class);
      promise.setSuccess(nodeConnection);
      return promise;
    });
    return nodeConnection;
  }

  private Node createNode() {
    return new Node(address, filters, weight, serviceName, proto, ssl, bootstrap, connectionPool);
  }
}
