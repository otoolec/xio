package com.xjeffrose.xio.client.loadbalancer.connection;

import com.xjeffrose.xio.client.XioConnectionPool;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class NodeConnectionPool {

  private final XioConnectionPool xioPool;
  private final ConcurrentHashMap<NodeConnection, Channel> leasedConnections =
    new ConcurrentHashMap<>();

  public NodeConnectionPool(XioConnectionPool xioPool) {
    this.xioPool = xioPool;
  }

  public Future<NodeConnection> acquireConnection(final Promise<NodeConnection> connectionPromise) {
    xioPool.acquire().addListener(new FutureListener<Channel>() {
      @Override
      public void operationComplete(Future<Channel> future) throws Exception {
        if (future.isSuccess()) {
          Channel channel = future.getNow();
          NodeConnection nodeConnection = new ChannelNodeConnection(channel);
          leasedConnections.put(nodeConnection, channel);
          connectionPromise.setSuccess(nodeConnection);
        } else {
          connectionPromise.setFailure(future.cause());
        }
      }
    });
    return connectionPromise;
  }

  public Optional<Future<Void>> releaseConnection(NodeConnection nodeConnection) {
    if (leasedConnections.containsKey(nodeConnection)) {
      Channel channel = leasedConnections.remove(nodeConnection);
      return Optional.of(xioPool.release(channel));
    }
    return Optional.empty();
  }
}
