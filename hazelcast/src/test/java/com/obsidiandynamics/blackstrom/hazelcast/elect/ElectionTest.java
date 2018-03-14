package com.obsidiandynamics.blackstrom.hazelcast.elect;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class ElectionTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
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
  
  private Election newElection(ElectionConfig config, IMap<String, byte[]> leaseTable, LeaseChangeHandler assignmentHandler) {
    final Election election = new Election(config, leaseTable, assignmentHandler);
    elections.add(election);
    return election;
  }
  
  private static LeaseChangeHandler mockHandler() {
    return mock(LeaseChangeHandler.class);
  }
  
  private static IMap<String, byte[]> leaseTable(HazelcastInstance instance) {
    return instance.getMap("sys.lease");
  }

  @Test
  public void testSingleNodeEmptyWithNoCandidates() {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);
    
    TestSupport.sleep(10);
    assertEquals(0, e.getLeaseView().asMap().size());
    verify(handler, never()).onAssign(any(), any());
    verify(handler, never()).onExpire(any(), any());
  }

  @Test
  public void testSingleNodeExpiredWithNoCandidates() {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final UUID o = UUID.randomUUID();
    leaseTable(h).put("resource", new Lease(o, 0).pack());
    final UUID c = UUID.randomUUID();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);
    doAnswer(invocation -> {
      e.getRegister().unenroll("resource", c);
      return null;
    }).when(handler).onExpire(any(), any());
    e.getRegister().enroll("resource", c);
    
    await.until(() -> assertEquals(1, e.getLeaseView().asMap().size()));
    verify(handler, never()).onAssign(eq("resource"), eq(c));
    verify(handler, atLeastOnce()).onExpire(eq("resource"), eq(o));
  }

  @Test
  public void testSingleNodeElectFromVacantAndTouch() throws NotLeaderException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final int leaseDuration = 60_000;
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                   leaseTable(h), handler);
    
    final UUID c = UUID.randomUUID();
    final long beforeElection = System.currentTimeMillis();
    e.getRegister().enroll("resource", c);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c)));
    assertEquals(1, e.getLeaseView().asMap().size());
    await.until(() -> verify(handler).onAssign(eq("resource"), eq(c)));
    verify(handler, atLeastOnce()).onExpire(eq("resource"), isNull());
    final Lease lease0 = e.getLeaseView().asMap().get("resource");
    assertEquals(c, lease0.getTenant());
    assertTrue(lease0.getExpiry() >= beforeElection + leaseDuration);
    
    TestSupport.sleep(10);
    final long beforeTouch = System.currentTimeMillis();
    e.touch("resource", c);
    final Lease lease1 = e.getLeaseView().asMap().get("resource");
    assertEquals(c, lease1.getTenant());
    assertTrue(lease1.getExpiry() >= beforeTouch + leaseDuration);
    assertTrue(lease1.getExpiry() > lease0.getExpiry());
  }

  @Test
  public void testSingleNodeElectFromVacantMissed() throws NotLeaderException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final int leaseDuration = 60_000;
    final IMap<String, byte[]> leaseTable = leaseTable(h);
    
    final IMap<String, byte[]> leaseTableSpied = spy(leaseTable);
    // intercept putIfAbsent() and make it fail by pretending that a value was already present
    doAnswer(invocation -> {
      return new byte[0];
    }).when(leaseTableSpied).putIfAbsent(any(), any());
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                   leaseTableSpied, handler);
    
    final UUID c = UUID.randomUUID();
    e.getRegister().enroll("resource", c);
    await.until(() -> verify(leaseTableSpied, atLeast(2)).putIfAbsent(any(), any()));
  }

  @Test
  public void testSingleNodeElectFromOtherAndTouch() throws NotLeaderException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final int leaseDuration = 60_000;
    leaseTable(h).put("resource", new Lease(UUID.randomUUID(), 0).pack());
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                   leaseTable(h), handler);
    
    final UUID c = UUID.randomUUID();
    final long beforeElection = System.currentTimeMillis();
    e.getRegister().enroll("resource", c);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c)));
    assertEquals(1, e.getLeaseView().asMap().size());
    await.until(() -> verify(handler).onAssign(eq("resource"), eq(c)));
    final Lease lease = e.getLeaseView().asMap().get("resource");
    assertEquals(c, lease.getTenant());
    assertTrue(lease.getExpiry() >= beforeElection + leaseDuration);
  }
  
  @Test(expected=NotLeaderException.class)
  public void testSingleNodeTouchNotLeaderVacant() throws NotLeaderException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);

    final UUID c = UUID.randomUUID();
    e.touch("resource", c);
  }

  @Test(expected=NotLeaderException.class)
  public void testSingleNodeTouchNotLeaderOther() throws NotLeaderException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);
    
    final UUID c0 = UUID.randomUUID();
    e.getRegister().enroll("resource", c0);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c0)));
    assertEquals(1, e.getLeaseView().asMap().size());

    final UUID c1 = UUID.randomUUID();
    e.touch("resource", c1);
  }

  @Test(expected=NotLeaderException.class)
  public void testSingleNodeTouchNotLeaderBackgroundReElection() throws NotLeaderException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(60_000), leaseTable(h), handler);
    
    final UUID c0 = UUID.randomUUID();
    e.getRegister().enroll("resource", c0);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c0)));
    assertEquals(1, e.getLeaseView().asMap().size());

    final UUID c1 = UUID.randomUUID();
    leaseTable(h).put("resource", new Lease(c1, System.currentTimeMillis() + 60_000).pack());
    
    // according to the view snapshot, c0 is the leader, but behind the scenes we've elected c1
    assertTrue(e.getLeaseView().isCurrentTenant("resource", c0));
    e.touch("resource", c0);
  }
}
