package com.xjeffrose.xio.client.loadbalancer.connection;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.Future;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * TODO: Change response to ConnectionFuture TODO: Add a way to register a read event listener
 */
public class ChannelNodeConnection implements NodeConnection {

  public static final String CHANNEL_NODE_CONNECTION_READ = "ChannelNodeConnection.read";
  private final Channel channel;

  private final Queue<NodeReadListener> listenerQueue = new ConcurrentLinkedQueue<>();

  public ChannelNodeConnection(Channel channel) {
    this.channel = channel;
    registerInboundHandler();
  }

  @Override
  public Future<Void> write(Object message) {
    return channel.write(message);
  }

  @Override
  public void flush() {
    channel.flush();
  }

  @Override
  public ChannelFuture writeAndFlush(Object message) {
    return channel.writeAndFlush(message);
  }

  @Override public void addReadListener(NodeReadListener listener) {
    listenerQueue.add(listener);
  }

  @Override public void close() {
    removeInboundHandler();
  }

  protected void registerInboundHandler() {
    channel.pipeline().addLast(CHANNEL_NODE_CONNECTION_READ, new ChannelInboundHandlerAdapter() {
      @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // TODO: Need to reason about thread safety and whether these should be asynchronous
        listenerQueue.forEach(listener -> listener.readEvent(msg));
        super.channelRead(ctx, msg);
      }
    });
  }

  protected void removeInboundHandler() {
    channel.pipeline().remove(CHANNEL_NODE_CONNECTION_READ);
  }
}

