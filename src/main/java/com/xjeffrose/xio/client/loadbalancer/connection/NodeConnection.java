package com.xjeffrose.xio.client.loadbalancer.connection;

import io.netty.util.concurrent.Future;

/**
 *
 */
public interface NodeConnection {

  Future<Void> write(Object message);

  void flush();

  Future<Void> writeAndFlush(Object message);

  void addReadListener(NodeReadListener listener);

  void close();
}
