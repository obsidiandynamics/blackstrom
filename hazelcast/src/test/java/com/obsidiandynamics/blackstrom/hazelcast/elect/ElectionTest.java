package com.obsidiandynamics.blackstrom.hazelcast.elect;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

public final class ElectionTest {
  private final Set<HazelcastInstance> instances = new HashSet<>();
  
  private final Set<Election> elections = new HashSet<>();
  
  private final Timesert await = Wait.SHORT;
  
  @After
  public void after() {
    elections.forEach(e -> e.terminate());
    elections.forEach(e -> e.joinQuietly());
    instances.forEach(h -> h.getLifecycleService().terminate());
  }
  
  private HazelcastInstance newInstance() {
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "slf4j");
    final HazelcastInstance instance = new MockHazelcastInstanceFactory().create(config);
    instances.add(instance);
    return instance;
  }
  
  private Election newElection(ElectionConfig config, IMap<String, byte[]> leaseTable, LeadershipAssignmentHandler assignmentHandler) {
    final Election election = new Election(config, leaseTable, assignmentHandler);
    elections.add(election);
    return election;
  }
  
  private static LeadershipAssignmentHandler mockHandler() {
    return mock(LeadershipAssignmentHandler.class);
  }
  
  private static IMap<String, byte[]> leaseTable(HazelcastInstance instance) {
    return instance.getMap("sys.lease");
  }

  @Test
  public void testSingleNodeEmpty() {
    final HazelcastInstance h = newInstance();
    final LeadershipAssignmentHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);
    
    TestSupport.sleep(10);
    assertEquals(0, e.getLeaseView().asMap().size());
    verify(handler, never()).onAssign(any(), any());
  }

  @Test
  public void testSingleNodeElect() {
    final HazelcastInstance h = newInstance();
    final LeadershipAssignmentHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);
    
    final UUID c = UUID.randomUUID();
    e.getRegister().enroll("resource", c);
    await.until(() -> assertTrue(e.getLeaseView().isLeader("resource", c)));
    assertEquals(1, e.getLeaseView().asMap().size());
    await.until(() -> verify(handler).onAssign(any(), any()));
  }
  
  @Test(expected=NotLeaderException.class)
  public void testSingleNodeTouchNotLeaderVacant() throws NotLeaderException {
    final HazelcastInstance h = newInstance();
    final LeadershipAssignmentHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);

    final UUID c = UUID.randomUUID();
    e.touch("resource", c);
  }
}
