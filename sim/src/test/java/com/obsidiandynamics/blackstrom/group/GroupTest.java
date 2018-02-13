package com.obsidiandynamics.blackstrom.group;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.jgroups.*;
import org.jgroups.util.*;
import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class GroupTest {
  private static final ChannelFactory CHANNEL_FACTORY = () -> Group.newChannel(Util.getLocalhost());
  
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
  
  private static Runnable viewSize(int size, JChannel channel) {
    return () -> assertEquals(size, channel.getView().size());
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
    
    wait.until(viewSize(2, g0.channel()));
    wait.until(viewSize(2, g1.channel()));
    
    g0.send(new Message(null, "test"));
    wait.until(received("test", g1Received));
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
    final HostMessageHandler g1Handler = (chan, m) -> g1Received.set(m.getObject());
    
    final UUID handledId = UUID.randomUUID();
    final UUID unhandledId = UUID.randomUUID();
    final Group g0 = create().withHandler(handledId, g0Handler).connect(cluster);
    final Group g1 = create().withHandler(handledId, g1Handler).connect(cluster);
    assertEquals(1, g0.numHandlers(handledId));
    assertEquals(1, g1.numHandlers(handledId));
    
    wait.until(viewSize(2, g0.channel()));
    wait.until(viewSize(2, g1.channel()));

    final TestMessage unhandledMessage = new TestMessage(unhandledId);
    final TestMessage handledMessage = new TestMessage(handledId);
    g0.send(new Message(null, handledMessage));
    g0.send(new Message(null, unhandledMessage));
    wait.until(received(handledMessage, g1Received));
    assertNull(g0Received.get());
    
    g0.removeHandler(handledId, g0Handler);
    g1.removeHandler(handledId, g1Handler);
    
    assertEquals(0, g0.numHandlers(handledId));
    assertEquals(0, g1.numHandlers(handledId));
  }
  
  @Test
  public void testRequest() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    final Group g0 = create().connect(cluster);
    final Group g1 = create().connect(cluster);
    
    wait.until(viewSize(2, g0.channel()));
    wait.until(viewSize(2, g1.channel()));
    
    g1.withHandler((chan, m) -> {
      final Object obj = m.getObject();
      if (obj instanceof SyncMessage) {
        chan.send(new Message(m.getSrc(), Ack.of(((SyncMessage) obj))));
      }
    });
    
    final Address address = peer(g0.channel());
    final UUID messageId = UUID.randomUUID();
    final Future<Message> f = g0.request(address, new TestMessage(messageId));
    final Message response = f.get();
    assertEquals(Ack.forId(messageId), response.getObject());
  }
  
  private static Address peer(JChannel channel) {
    final Address current = channel.getAddress();
    final Set<Address> addresses = new HashSet<>(channel.getView().getMembers());
    assertEquals("addresses=" + addresses, 2, addresses.size());
    final boolean removed = addresses.remove(current);
    assertTrue(removed);
    return addresses.iterator().next();
  }
}
