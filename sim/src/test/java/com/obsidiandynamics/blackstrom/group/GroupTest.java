package com.obsidiandynamics.blackstrom.group;

import static org.jgroups.Message.Flag.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.*;
import java.util.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.jgroups.*;
import org.jgroups.util.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.slf4j.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.util.select.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class GroupTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private static final ChannelFactory UDP_FACTORY = () -> Group.newUdpChannel(Util.getLocalhost());
  private static final ChannelFactory MOCK_FACTORY = () -> Group.newLoopbackChannel();
  
  private static final boolean MOCK = true;
  
  private static final ChannelFactory CHANNEL_FACTORY = MOCK ? MOCK_FACTORY : UDP_FACTORY;
  
  private static class TestPacket extends SyncPacket {
    private static final long serialVersionUID = 1L;
    
    TestPacket(Serializable id) {
      super(id);
    }
  }
  
  private final Set<Group> groups = new HashSet<>();
  
  private final Timesert wait = Wait.SHORT;
  
  @After
  public void after() {
    groups.forEach(g -> g.close());
    groups.clear();
  }
  
  private Group create() throws Exception {
    return create(CHANNEL_FACTORY);
  }
  
  private Group create(ChannelFactory factory) throws Exception {
    final Group g = new Group(factory.create());
    groups.add(g);
    return g;
  }
  
  private static Runnable viewSize(int size, Group group) {
    return () -> assertEquals(size, group.view().size());
  }
  
  private static <T> Runnable received(T expected, AtomicReference<T> ref) {
    return () -> assertEquals(expected, ref.get());
  }
  
  private static <P extends SyncPacket> ThrowingConsumer<P> ack(JChannel chan, Message m) {
    return p -> chan.send(new Message(m.getSrc(), Ack.of(p)).setFlag(DONT_BUNDLE));
  }
  
  @Test
  public void testFactories() throws Exception {
    create(UDP_FACTORY);
    create(MOCK_FACTORY);
  }
  
  @Test
  public void testGeneralHandler() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    
    final AtomicReference<String> g0Received = new AtomicReference<>();
    final AtomicReference<String> g1Received = new AtomicReference<>();
    final HostMessageHandler g0Handler = (chan, m) -> g0Received.set(m.getObject());
    final HostMessageHandler g1Handler = (chan, m) -> g1Received.set(m.getObject());
    final Group g0 = create().withHandler(g0Handler).connect(cluster);
    final Group g1 = create().withHandler(g1Handler).connect(cluster);
    assertEquals(1, g0.numHandlers());
    assertEquals(1, g1.numHandlers());
    assertNotNull(g0.channel());
    
    final Logger l0 = mock(Logger.class);
    final Logger l1 = mock(Logger.class);
    when(l0.isDebugEnabled()).thenReturn(true);
    when(l0.isDebugEnabled()).thenReturn(false);
    g0.withLogger(l0);
    g1.withLogger(l1);
    
    wait.until(viewSize(2, g0));
    wait.until(viewSize(2, g1));
    
    g0.send(new Message(null, "test").setFlag(DONT_BUNDLE));
    wait.until(received("test", g1Received));
    assertNotNull(g1Received.get());
    assertNull(g0Received.get());
    
    g0.removeHandler(g0Handler);
    g1.removeHandler(g1Handler);
    
    assertEquals(0, g0.numHandlers());
    assertEquals(0, g1.numHandlers());
  }
  
  @Test
  public void testHandlerError() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    
    final Group g0 = create().connect(cluster);
    final Group g1 = create().withHandler((chan, m) -> {
      throw new Exception("testHandlerError");
    }).connect(cluster);
    
    final Logger l1 = mock(Logger.class);
    g1.withLogger(l1);
    
    wait.until(viewSize(2, g0));
    wait.until(viewSize(2, g1));
    
    g0.send(new Message(null, "test").setFlag(DONT_BUNDLE));
    wait.until(() -> {
      verify(l1).warn(notNull(), (Throwable) notNull());
    });
  }
  
  @Test
  public void testIdHandler() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    
    final AtomicReference<SyncPacket> g0Received = new AtomicReference<>();
    final AtomicReference<SyncPacket> g1Received = new AtomicReference<>();
    final HostMessageHandler g0Handler = (chan, m) -> g0Received.set(m.getObject());
    final HostMessageHandler g1HandlerA = (chan, m) -> g1Received.set(m.getObject());
    final HostMessageHandler g1HandlerB = (chan, m) -> g1Received.set(m.getObject());
    
    final UUID handledId = UUID.randomUUID();
    final UUID unhandledId = UUID.randomUUID();
    final Group g0 = create().withHandler(handledId, g0Handler).connect(cluster);
    final Group g1 = create().withHandler(handledId, g1HandlerA).withHandler(handledId, g1HandlerB).connect(cluster);
    assertEquals(1, g0.numHandlers(handledId));
    assertEquals(2, g1.numHandlers(handledId));
    
    wait.until(viewSize(2, g0));
    wait.until(viewSize(2, g1));

    final TestPacket unhandledPacket = new TestPacket(unhandledId);
    final TestPacket handledPacket = new TestPacket(handledId);
    g0.send(new Message(null, handledPacket).setFlag(DONT_BUNDLE));
    g0.send(new Message(null, unhandledPacket).setFlag(DONT_BUNDLE));
    wait.until(received(handledPacket, g1Received));
    assertNull(g0Received.get());
    
    g0.removeHandler(handledId, g0Handler);
    g1.removeHandler(handledId, g1HandlerA);
    
    assertEquals(0, g0.numHandlers(handledId));
    assertEquals(1, g1.numHandlers(handledId));
    
    g1.removeHandler(handledId, g1HandlerB);
    assertEquals(0, g1.numHandlers(handledId));
  }
  
  @Test
  public void testRequestResponseFuture() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    final Group g0 = create().connect(cluster);
    final Group g1 = create().connect(cluster);
    
    wait.until(viewSize(2, g0));
    wait.until(viewSize(2, g1));
    
    final HostMessageHandler handler = 
        (chan, m) -> Select.fromThrowing(m.getObject()).whenInstanceOf(TestPacket.class).checked().then(ack(chan, m));
    g0.withHandler(handler);
    g1.withHandler(handler);
    
    final Address address = g0.peer();
    final UUID packetId = UUID.randomUUID();
    final Future<Message> f = g0.request(address, new TestPacket(packetId), DONT_BUNDLE);
    final Message response;
    try {
      response = f.get();
    } finally {
      assertEquals(0, g0.numHandlers(packetId));
      f.cancel(false);
    }
    assertEquals(Ack.forId(packetId), response.getObject());
  }
  
  @Test
  public void testRequestResponseCallback() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    final Group g0 = create().connect(cluster);
    final Group g1 = create().connect(cluster);
    
    wait.until(viewSize(2, g0));
    wait.until(viewSize(2, g1));
    
    final HostMessageHandler handler = 
        (chan, m) -> Select.fromThrowing(m.getObject()).whenInstanceOf(TestPacket.class).checked().then(ack(chan, m));
    g0.withHandler(handler);
    g1.withHandler(handler);
    
    final Address address = g0.peer();
    final UUID packetId = UUID.randomUUID();
    final AtomicReference<Message> callbackRef = new AtomicReference<>();
    final ResponseSync responseSync = g0.request(address, new TestPacket(packetId), (chan, resp) -> callbackRef.set(resp), DONT_BUNDLE);
    wait.untilTrue(() -> callbackRef.get() != null);
    assertEquals(0, g0.numHandlers(packetId));
    assertNotNull(callbackRef.get());
    assertEquals(Ack.forId(packetId), callbackRef.get().getObject());
    responseSync.cancel();
  }
  
  @Test(expected=TimeoutException.class)
  public void testRequestFutureTimeout() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    final Group g0 = create().connect(cluster);
    final Group g1 = create().connect(cluster);
    
    wait.until(viewSize(2, g0));
    wait.until(viewSize(2, g1));
    
    final Address address = g0.peer();
    final UUID packetId = UUID.randomUUID();
    final Future<Message> f = g0.request(address, new TestPacket(packetId), DONT_BUNDLE);
    try {
      f.get(1, TimeUnit.MILLISECONDS);
    } finally {
      assertEquals(1, g0.numHandlers(packetId));
      f.cancel(false);
      assertEquals(0, g0.numHandlers(packetId));
    }
  }
  
  @Test
  public void testGatherResponseFuture() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    final Group g0 = create().connect(cluster);
    final Group g1 = create().connect(cluster);
    final Group g2 = create().connect(cluster);
    
    wait.until(viewSize(3, g0));
    wait.until(viewSize(3, g1));
    wait.until(viewSize(3, g2));
    
    final HostMessageHandler handler = 
        (chan, m) -> Select.fromThrowing(m.getObject()).whenInstanceOf(TestPacket.class).checked().then(ack(chan, m));
    g0.withHandler(handler);
    g1.withHandler(handler);
    g2.withHandler(handler);
    
    final UUID packetId = UUID.randomUUID();
    final Future<Map<Address, Message>> f = g0.gather(new TestPacket(packetId), DONT_BUNDLE);
    final Map<Address, Message> responses;
    try {
      responses = f.get();
    } finally {
      assertEquals(0, g0.numHandlers(packetId));
      f.cancel(false);
    }
    assertEquals(2, responses.size());
    assertEquals(g0.peers(), responses.keySet());
  }
  
  @Test
  public void testGatherResponseCallback() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    final Group g0 = create().connect(cluster);
    final Group g1 = create().connect(cluster);
    final Group g2 = create().connect(cluster);
    
    wait.until(viewSize(3, g0));
    wait.until(viewSize(3, g1));
    wait.until(viewSize(3, g2));
    
    final HostMessageHandler handler = 
        (chan, m) -> Select.fromThrowing(m.getObject()).whenInstanceOf(TestPacket.class).checked().then(ack(chan, m));
    g0.withHandler(handler);
    g1.withHandler(handler);
    g2.withHandler(handler);
    
    final UUID packetId = UUID.randomUUID();
    final AtomicReference<Map<Address, Message>> callbackRef = new AtomicReference<>();
    final ResponseSync responseSync = g0.gather(new TestPacket(packetId), (chan, messages) -> callbackRef.set(messages), DONT_BUNDLE);
    wait.untilTrue(() -> callbackRef.get() != null);
    responseSync.cancel();
    Assertions.assertToStringOverride(responseSync);
    assertEquals(0, g0.numHandlers(packetId));
    assertEquals(2, callbackRef.get().size());
    assertEquals(g0.peers(), callbackRef.get().keySet());
  }
  
  @Test(expected=TimeoutException.class)
  public void testGatherResponseFutureTimeout() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    final Group g0 = create().connect(cluster);
    final Group g1 = create().connect(cluster);
    final Group g2 = create().connect(cluster);
    
    wait.until(viewSize(3, g0));
    wait.until(viewSize(3, g1));
    wait.until(viewSize(3, g2));
    
    final UUID packetId = UUID.randomUUID();
    final Future<Map<Address, Message>> f = g0.gather(new TestPacket(packetId), DONT_BUNDLE);
    try {
      f.get(1, TimeUnit.MILLISECONDS);
    } finally {
      assertEquals(1, g0.numHandlers(packetId));
      f.cancel(false);
      assertEquals(0, g0.numHandlers(packetId));
    }
  }
}
