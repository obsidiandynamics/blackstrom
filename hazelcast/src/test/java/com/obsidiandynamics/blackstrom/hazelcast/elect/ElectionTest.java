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
  
  private static HazelcastProvider defaultProvider;
  
  @BeforeClass
  public static void beforeClass() {
    defaultProvider = new MockHazelcastProvider();
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
    return newInstance(defaultProvider);
  }
  
  private HazelcastInstance newInstance(HazelcastProvider provider) {
    final Config config = new Config()
        .setProperty("hazelcast.logging.type", "none");
    final HazelcastInstance instance = provider.createInstance(config);
    instances.add(instance);
    return instance;
  }
  
  private Election newElection(ElectionConfig config, IMap<String, byte[]> leaseTable, LeaseChangeHandler changeHandler) {
    final Election election = new Election(config, leaseTable, changeHandler);
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
      e.getRegistry().unenroll("resource", c);
      return null;
    }).when(handler).onExpire(any(), any());
    e.getRegistry().enroll("resource", c);
    
    await.until(() -> assertEquals(1, e.getLeaseView().asMap().size()));
    verify(handler, never()).onAssign(eq("resource"), eq(c));
    await.until(() -> verify(handler, atLeastOnce()).onExpire(eq("resource"), eq(o)));
  }

  @Test
  public void testSingleNodeElectFromVacantAndTouchYield() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final int leaseDuration = 60_000;
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                   leaseTable(h), handler);
    
    final UUID c = UUID.randomUUID();
    final long beforeElection = System.currentTimeMillis();
    e.getRegistry().enroll("resource", c);
    await.until(() -> {
      assertTrue(e.getLeaseView().isCurrentTenant("resource", c));
      assertEquals(1, e.getLeaseView().asMap().size());
      verify(handler).onAssign(eq("resource"), eq(c));
      verify(handler, atLeastOnce()).onExpire(eq("resource"), isNull());
      final Lease lease = e.getLeaseView().asMap().get("resource");
      assertEquals(c, lease.getTenant());
      assertTrue(lease.getExpiry() >= beforeElection + leaseDuration);
    });
    
    TestSupport.sleep(10);
    final long beforeTouch = System.currentTimeMillis();
    e.touch("resource", c);
    await.until(() -> {
      final Lease lease = e.getLeaseView().asMap().get("resource");
      assertEquals(c, lease.getTenant());
      assertTrue("beforeTouch=" + beforeTouch + ", leaseExpiry=" + lease.getExpiry(), 
                 lease.getExpiry() >= beforeTouch + leaseDuration);
    });
    
    e.getRegistry().unenroll("resource", c);
    e.yield("resource", c);
    await.until(() -> {
      assertEquals(0, e.getLeaseView().asMap().size());
    });
    
    // should be no further elections, since we unenrolled before yielding
    TestSupport.sleep(10);
    assertEquals(0, e.getLeaseView().asMap().size());
  }
  
  @Test
  public void testTwoNodesElectOneCandidateFromVacant() throws NotTenantException {
    final int leaseDuration = 60_000;
    
    final HazelcastInstance h0 = newInstance();
    final LeaseChangeHandler handler0 = mockHandler();
    final Election e0 = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                    leaseTable(h0), handler0);

    final HazelcastInstance h1 = newInstance();
    final LeaseChangeHandler handler1 = mockHandler();
    final Election e1 = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                    leaseTable(h1), handler1);
    
    final UUID c = UUID.randomUUID();
    final long beforeElection = System.currentTimeMillis();
    e0.getRegistry().enroll("resource", c);
    e1.getRegistry().enroll("resource", c);
    await.until(() -> {
      final Lease lease0 = e0.getLeaseView().asMap().get("resource");
      assertNotNull(lease0);
      assertEquals(c, lease0.getTenant());
      assertTrue(lease0.getExpiry() >= beforeElection + leaseDuration);
      
      final Lease lease1 = e1.getLeaseView().asMap().get("resource");
      assertNotNull(lease1);
      assertEquals(c, lease1.getTenant());
      assertTrue(lease1.getExpiry() >= beforeElection + leaseDuration);
    });
    
    TestSupport.sleep(10);
    final long beforeTouch = System.currentTimeMillis();
    e0.touch("resource", c);
    await.until(() -> {
      final Lease lease1 = e1.getLeaseView().asMap().get("resource");
      assertEquals(c, lease1.getTenant());
      assertTrue("beforeTouch=" + beforeTouch + ", leaseExpiry=" + lease1.getExpiry(), 
                 lease1.getExpiry() >= beforeTouch + leaseDuration);
    });
  }

  @Test
  public void testSingleNodeElectFromVacantMissed() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final int leaseDuration = 60_000;
    final IMap<String, byte[]> leaseTable = leaseTable(h);
    
    final IMap<String, byte[]> leaseTableSpied = spy(leaseTable);
    // intercept putIfAbsent() and make it fail by pretending that a value was already present
    doAnswer(invocation -> new byte[0]).when(leaseTableSpied).putIfAbsent(any(), any());
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                   leaseTableSpied, handler);
    
    final UUID c = UUID.randomUUID();
    e.getRegistry().enroll("resource", c);
    await.until(() -> verify(leaseTableSpied, atLeast(2)).putIfAbsent(any(), any()));
  }

  @Test
  public void testSingleNodeElectFromOtherAndTouch() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final int leaseDuration = 60_000;
    leaseTable(h).put("resource", new Lease(UUID.randomUUID(), 0).pack());
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                   leaseTable(h), handler);
    
    final UUID c = UUID.randomUUID();
    final long beforeElection = System.currentTimeMillis();
    e.getRegistry().enroll("resource", c);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c)));
    assertEquals(1, e.getLeaseView().asMap().size());
    await.until(() -> verify(handler).onAssign(eq("resource"), eq(c)));
    final Lease lease = e.getLeaseView().asMap().get("resource");
    assertEquals(c, lease.getTenant());
    assertTrue(lease.getExpiry() >= beforeElection + leaseDuration);
  }
  
  @Test(expected=NotTenantException.class)
  public void testSingleNodeTouchNotTenantVacant() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);

    final UUID c = UUID.randomUUID();
    e.touch("resource", c);
  }

  @Test(expected=NotTenantException.class)
  public void testSingleNodeTouchNotTenantOther() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);
    
    final UUID c0 = UUID.randomUUID();
    e.getRegistry().enroll("resource", c0);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c0)));
    assertEquals(1, e.getLeaseView().asMap().size());

    final UUID c1 = UUID.randomUUID();
    e.touch("resource", c1);
  }

  @Test(expected=NotTenantException.class)
  public void testSingleNodeTouchNotTenantBackgroundReElection() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(60_000), leaseTable(h), handler);
    
    final UUID c0 = UUID.randomUUID();
    e.getRegistry().enroll("resource", c0);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c0)));
    assertEquals(1, e.getLeaseView().asMap().size());

    final UUID c1 = UUID.randomUUID();
    leaseTable(h).put("resource", new Lease(c1, System.currentTimeMillis() + 60_000).pack());
    
    // according to the view snapshot, c0 is the leader, but behind the scenes we've elected c1
    assertTrue(e.getLeaseView().isCurrentTenant("resource", c0));
    e.touch("resource", c0);
  }

  @Test(expected=NotTenantException.class)
  public void testSingleNodeYieldNotTenantVacant() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);

    final UUID c = UUID.randomUUID();
    e.yield("resource", c);
  }

  @Test(expected=NotTenantException.class)
  public void testSingleNodeYieldNotTenantBackgroundReElection() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final LeaseChangeHandler handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(60_000), leaseTable(h), handler);
    
    final UUID c0 = UUID.randomUUID();
    e.getRegistry().enroll("resource", c0);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c0)));
    assertEquals(1, e.getLeaseView().asMap().size());

    final UUID c1 = UUID.randomUUID();
    leaseTable(h).put("resource", new Lease(c1, System.currentTimeMillis() + 60_000).pack());
    
    // according to the view snapshot, c0 is the leader, but behind the scenes we've elected c1
    assertTrue(e.getLeaseView().isCurrentTenant("resource", c0));
    e.yield("resource", c0);
  }
}
