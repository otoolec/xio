package com.xjeffrose.xio.client.loadbalancer.connection;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ChannelNodeConnectionTest {

  private EmbeddedChannel channel;
  private NodeConnection connection;
  private ByteBuf message;

  @Before
  public void setUp() throws Exception {
    channel = new EmbeddedChannel();
    connection = new ChannelNodeConnection(channel);
    message = Unpooled.copiedBuffer("Test".getBytes());
  }

  @Test
  public void testWrite() throws Exception {
    connection.write(message);

    assertEquals(0, channel.outboundMessages().size());

    connection.flush();

    assertEquals(message, channel.outboundMessages().poll());
  }

  @Test
  public void testWrite_Listener() throws Exception {
    final Future<Void> writeFuture = connection.write(message);
    final AtomicBoolean eventReceived = new AtomicBoolean(false);
    writeFuture.addListener(future -> {
      assertEquals(writeFuture, future);
      eventReceived.compareAndSet(false, true);
    });

    assertEquals(0, channel.outboundMessages().size());

    connection.flush();

    assertEquals(message, channel.outboundMessages().poll());

    assertTrue(eventReceived.get());
  }

  @Test
  public void testWriteAndFlush() throws Exception {
    connection.writeAndFlush(message);

    assertEquals(message, channel.outboundMessages().poll());
  }

  @Test
  public void testAddReadListener() {
    final AtomicReference<Object> messageReceived = new AtomicReference<>(null);
    connection.addReadListener(messageReceived::set);

    channel.writeInbound(message);

    assertEquals(message, messageReceived.get());
  }
}
