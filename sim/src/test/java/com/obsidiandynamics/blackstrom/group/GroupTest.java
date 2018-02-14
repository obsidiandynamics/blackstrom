package com.obsidiandynamics.blackstrom.group;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.*;
import java.util.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.jgroups.*;
import org.jgroups.Message.*;
import org.jgroups.util.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.mockito.*;
import org.slf4j.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;
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
    
    g0.send(new Message(null, "test").setFlag(Flag.DONT_BUNDLE));
    wait.until(received("test", g1Received));
    assertNotNull(g1Received.get());
    assertNull(g0Received.get());
    
    g0.removeHandler(g0Handler);
    g1.removeHandler(g1Handler);
    
    assertEquals(0, g0.numHandlers());
    assertEquals(0, g1.numHandlers());
  }
  
  private static class TestMessage extends SyncMessage {
    private static final long serialVersionUID = 1L;
    
    TestMessage(Serializable id) {
      super(id);
    }
  }
  
  @Test
  public void testIdHandler() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    
    final AtomicReference<SyncMessage> g0Received = new AtomicReference<>();
    final AtomicReference<SyncMessage> g1Received = new AtomicReference<>();
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

    final TestMessage unhandledMessage = new TestMessage(unhandledId);
    final TestMessage handledMessage = new TestMessage(handledId);
    g0.send(new Message(null, handledMessage));
    g0.send(new Message(null, unhandledMessage));
    wait.until(received(handledMessage, g1Received));
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
    
    final HostMessageHandler handler = (chan, m) -> {
      final Object obj = m.getObject();
      if (obj instanceof TestMessage) {
        chan.send(new Message(m.getSrc(), Ack.of(((SyncMessage) obj))));
      }
    };
    g0.withHandler(handler);
    g1.withHandler(handler);
    
    final Address address = g0.peer();
    final UUID messageId = UUID.randomUUID();
    final Future<Message> f = g0.request(address, new TestMessage(messageId));
    final Message response;
    try {
      response = f.get();
    } finally {
      assertEquals(0, g0.numHandlers(messageId));
      f.cancel(false);
    }
    assertEquals(Ack.forId(messageId), response.getObject());
  }
  
  @Test
  public void testRequestResponseCallback() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    final Group g0 = create().connect(cluster);
    final Group g1 = create().connect(cluster);
    
    wait.until(viewSize(2, g0));
    wait.until(viewSize(2, g1));
    
    final HostMessageHandler handler = (chan, m) -> {
      final Object obj = m.getObject();
      if (obj instanceof TestMessage) {
        chan.send(new Message(m.getSrc(), Ack.of(((SyncMessage) obj))));
      }
    };
    g0.withHandler(handler);
    g1.withHandler(handler);
    
    final Address address = g0.peer();
    final UUID messageId = UUID.randomUUID();
    final AtomicReference<Message> callbackRef = new AtomicReference<>();
    g0.request(address, new TestMessage(messageId), (chan, resp) -> callbackRef.set(resp));
    wait.untilTrue(() -> callbackRef.get() != null);
    assertEquals(0, g0.numHandlers(messageId));
    assertNotNull(callbackRef.get());
    assertEquals(Ack.forId(messageId), callbackRef.get().getObject());
  }
  
  @Test(expected=TimeoutException.class)
  public void testRequestFutureTimeout() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    final Group g0 = create().connect(cluster);
    final Group g1 = create().connect(cluster);
    
    wait.until(viewSize(2, g0));
    wait.until(viewSize(2, g1));
    
    final Address address = g0.peer();
    final UUID messageId = UUID.randomUUID();
    final Future<Message> f = g0.request(address, new TestMessage(messageId));
    try {
      f.get(1, TimeUnit.MILLISECONDS);
    } finally {
      assertEquals(1, g0.numHandlers(messageId));
      f.cancel(false);
      assertEquals(0, g0.numHandlers(messageId));
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
    
    final HostMessageHandler handler = (chan, m) -> {
      final Object obj = m.getObject();
      if (obj instanceof TestMessage) {
        chan.send(new Message(m.getSrc(), Ack.of(((SyncMessage) obj))));
      }
    };
    g0.withHandler(handler);
    g1.withHandler(handler);
    g2.withHandler(handler);
    
    final UUID messageId = UUID.randomUUID();
    final Future<Map<Address, Message>> f = g0.gather(new TestMessage(messageId));
    final Map<Address, Message> responses;
    try {
      responses = f.get();
    } finally {
      assertEquals(0, g0.numHandlers(messageId));
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
    
    final HostMessageHandler handler = (chan, m) -> {
      final Object obj = m.getObject();
      if (obj instanceof TestMessage) {
        chan.send(new Message(m.getSrc(), Ack.of(((SyncMessage) obj))));
      }
    };
    g0.withHandler(handler);
    g1.withHandler(handler);
    g2.withHandler(handler);
    
    final UUID messageId = UUID.randomUUID();
    final AtomicReference<Map<Address, Message>> callbackRef = new AtomicReference<>();
    g0.gather(new TestMessage(messageId), (chan, messages) -> callbackRef.set(messages));
    wait.untilTrue(() -> callbackRef.get() != null);
    assertEquals(0, g0.numHandlers(messageId));
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
    
    final UUID messageId = UUID.randomUUID();
    final Future<Map<Address, Message>> f = g0.gather(new TestMessage(messageId));
    try {
      f.get(1, TimeUnit.MILLISECONDS);
    } finally {
      assertEquals(1, g0.numHandlers(messageId));
      f.cancel(false);
      assertEquals(0, g0.numHandlers(messageId));
    }
  }
}
