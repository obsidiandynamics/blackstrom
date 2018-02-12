package com.obsidiandynamics.blackstrom.group;

import static org.junit.Assert.*;

import java.util.*;
import java.util.UUID;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.jgroups.*;
import org.jgroups.util.*;
import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class GroupTest {
  private final Set<Group> groups = new HashSet<>();
  
  private final Timesert wait = Wait.SHORT;
  
  @After
  public void after() {
    groups.forEach(g -> g.close());
    groups.clear();
  }
  
  private Group create(JChannel channel) throws Exception {
    final Group g = new Group(channel);
    groups.add(g);
    return g;
  }
  
  private static Runnable viewSize(int size, JChannel channel) {
    return () -> assertEquals(size, channel.getView().size());
  }
  
  private static BooleanSupplier received(AtomicReference<Message> ref) {
    return () -> ref.get() != null;
  }
  
  @Test
  public void testGeneralHandler() throws Exception {
    final String cluster = UUID.randomUUID().toString();
    
    final AtomicReference<Message> g0Received = new AtomicReference<>();
    final AtomicReference<Message> g1Received = new AtomicReference<>();
    final HostMessageHandler g0Handler = (chan, m) -> {
      g0Received.set(m);
    };
    final HostMessageHandler g1Handler = (chan, m) -> {
      g1Received.set(m);
    };
    
    final Group g0 = create(Group.newChannel(Util.getLocalhost())).withHandler(g0Handler).connect(cluster);
    final Group g1 = create(Group.newChannel(Util.getLocalhost())).withHandler(g1Handler).connect(cluster);
    
    assertEquals(1, g0.numHandlers());
    assertEquals(1, g1.numHandlers());
    
    wait.until(viewSize(2, g0.channel()));
    wait.until(viewSize(2, g1.channel()));
    
    g0.channel().send(new Message(null, "test"));
    wait.untilTrue(received(g1Received));
    assertNull(g0Received.get());
    
    g0.removeHandler(g0Handler);
    g1.removeHandler(g1Handler);
    
    assertEquals(0, g0.numHandlers());
    assertEquals(0, g1.numHandlers());
  }
}
