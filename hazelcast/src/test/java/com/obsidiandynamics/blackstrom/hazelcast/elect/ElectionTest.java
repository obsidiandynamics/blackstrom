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
  
  private HazelcastProvider defaultProvider;
  
  private final Set<HazelcastInstance> instances = new HashSet<>();
  
  private final Set<Election> elections = new HashSet<>();
  
  private final Timesert await = Wait.SHORT;
  
  @Before
  public void before() {
    defaultProvider = new TestHazelcastProvider();
  }
  
  @After
  public void after() {
    elections.forEach(e -> e.terminate());
    elections.forEach(e -> e.joinQuietly());
    instances.forEach(h -> h.shutdown());
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
  
  private Election newElection(ElectionConfig config, IMap<String, byte[]> leaseTable, ScavengeWatcher changeHandler) {
    final Election election = new Election(config, leaseTable, changeHandler);
    elections.add(election);
    return election;
  }
  
  private static ScavengeWatcher mockHandler() {
    return mock(ScavengeWatcher.class);
  }
  
  private static IMap<String, byte[]> leaseTable(HazelcastInstance instance) {
    return instance.getMap("sys.lease");
  }

  /**
   *  Clean slate with no candidates to elect.
   */
  @Test
  public void testSingleNodeEmptyWithNoCandidates() {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);
    
    TestSupport.sleep(10);
    assertEquals(0, e.getLeaseView().asMap().size());
    verify(handler, never()).onAssign(any(), any());
    verify(handler, never()).onExpire(any(), any());
  }

  /**
   *  Starting from an expired lease (artificially injected) with no candidates to elect.
   */
  @Test
  public void testSingleNodeExpiredWithNoCandidates() {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    final UUID o = UUID.randomUUID();
    leaseTable(h).put("resource", new Lease(o, 0).pack());
    final UUID c = UUID.randomUUID();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);
    doAnswer(invocation -> {
      e.getRegistry().unenrol("resource", c);
      return null;
    }).when(handler).onExpire(any(), any());
    e.getRegistry().enrol("resource", c);
    
    await.until(() -> assertEquals(1, e.getLeaseView().asMap().size()));
    verify(handler, never()).onAssign(eq("resource"), eq(c));
    await.until(() -> verify(handler, atLeastOnce()).onExpire(eq("resource"), eq(o)));
  }

  /**
   *  Single node trying to elect a candidate from a clean slate (no prior tenants).
   *  
   *  @throws NotTenantException
   */
  @Test
  public void testSingleNodeElectFromVacantAndTouchYield() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    final int leaseDuration = 60_000;
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                   leaseTable(h), handler);
    
    final UUID c = UUID.randomUUID();
    final long beforeElection = System.currentTimeMillis();
    e.getRegistry().enrol("resource", c);
    await.until(() -> {
      assertTrue(e.getLeaseView().isCurrentTenant("resource", c));
      assertEquals(1, e.getLeaseView().asMap().size());
      verify(handler).onAssign(eq("resource"), eq(c));
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
    
    e.getRegistry().unenrol("resource", c);
    e.yield("resource", c);
    await.until(() -> {
      assertEquals(0, e.getLeaseView().asMap().size());
    });
    
    // should be no further elections, since we unenrolled before yielding
    TestSupport.sleep(10);
    assertEquals(0, e.getLeaseView().asMap().size());
  }
  
  /**
   *  Two nodes, both trying to elect the same candidate.
   *  
   *  @throws NotTenantException
   */
  @Test
  public void testTwoNodesOneCandidateElectFromVacant() throws NotTenantException {
    final int leaseDuration = 60_000;
    
    final HazelcastInstance h0 = newInstance();
    final ScavengeWatcher handler0 = mockHandler();
    final Election e0 = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                    leaseTable(h0), handler0);

    final HazelcastInstance h1 = newInstance();
    final ScavengeWatcher handler1 = mockHandler();
    final Election e1 = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                    leaseTable(h1), handler1);
    
    final UUID c = UUID.randomUUID();
    final long beforeElection = System.currentTimeMillis();
    e0.getRegistry().enrol("resource", c);
    e1.getRegistry().enrol("resource", c);
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

  /**
   *  Two nodes, each trying to elect its own candidate for the same resource.
   *  
   *  @throws NotTenantException
   */
  @Test
  public void testTwoNodesTwoCandidatesElectFromVacant() throws NotTenantException {
    final int leaseDuration = 60_000;
    
    final HazelcastInstance h0 = newInstance();
    final ScavengeWatcher handler0 = mockHandler();
    final Election e0 = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                    leaseTable(h0), handler0);

    final HazelcastInstance h1 = newInstance();
    final ScavengeWatcher handler1 = mockHandler();
    final Election e1 = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                    leaseTable(h1), handler1);
    
    final UUID c0 = UUID.randomUUID();
    final UUID c1 = UUID.randomUUID();
    final long beforeElection = System.currentTimeMillis();
    e0.getRegistry().enrol("resource", c0);
    e1.getRegistry().enrol("resource", c1);
    await.until(() -> {
      final Lease lease0 = e0.getLeaseView().asMap().get("resource");
      assertNotNull(lease0);
      assertTrue("lease0=" + lease0, lease0.getTenant().equals(c0) || lease0.getTenant().equals(c1));
      assertTrue(lease0.getExpiry() >= beforeElection + leaseDuration);
      
      final Lease lease1 = e1.getLeaseView().asMap().get("resource");
      assertNotNull(lease1);
      assertTrue("lease1=" + lease1, lease1.getTenant().equals(c0) || lease1.getTenant().equals(c1));
      assertTrue(lease1.getExpiry() >= beforeElection + leaseDuration);
    });
    
    TestSupport.sleep(10);
    final long beforeTouch = System.currentTimeMillis();
    final UUID tenant = e0.getLeaseView().getLease("resource").getTenant();
    e0.touch("resource", tenant);
    e1.touch("resource", tenant);
    await.until(() -> {
      final Lease lease0 = e0.getLeaseView().asMap().get("resource");
      assertEquals(tenant, lease0.getTenant());
      assertTrue("beforeTouch=" + beforeTouch + ", leaseExpiry=" + lease0.getExpiry(), 
                 lease0.getExpiry() >= beforeTouch + leaseDuration);
      
      final Lease lease1 = e1.getLeaseView().asMap().get("resource");
      assertEquals(tenant, lease1.getTenant());
      assertTrue("beforeTouch=" + beforeTouch + ", leaseExpiry=" + lease1.getExpiry(), 
                 lease1.getExpiry() >= beforeTouch + leaseDuration);
    });
  }

  /**
   *  Election of a single candidate from a clean slate, where a simulated background election (by
   *  a competing process) changes the contents of the lease table and thereby fails the CAS operation
   *  on the {@link IMap}. In other words, this simulates the race condition that a CAS is supposed to
   *  guard against.
   *  
   *  @throws NotTenantException
   */
  @Test
  public void testSingleNodeElectFromVacantMissed() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    final int leaseDuration = 60_000;
    final IMap<String, byte[]> leaseTable = leaseTable(h);
    
    final IMap<String, byte[]> leaseTableSpied = spy(leaseTable);
    // intercept putIfAbsent() and make it fail by pretending that a value was set
    doAnswer(invocation -> new byte[0]).when(leaseTableSpied).putIfAbsent(any(), any());
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                   leaseTableSpied, handler);
    
    final UUID c = UUID.randomUUID();
    e.getRegistry().enrol("resource", c);
    await.until(() -> verify(leaseTableSpied, atLeast(2)).putIfAbsent(any(), any()));
  }

  /**
   *  Tests the touching of a lease from a node that wasn't the initiator of the election. This
   *  first requires that the node update its lease view.
   *  
   *  @throws NotTenantException
   */
  @Test
  public void testSingleNodeElectFromOtherAndTouch() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    final int leaseDuration = 60_000;
    leaseTable(h).put("resource", new Lease(UUID.randomUUID(), 0).pack());
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1).withLeaseDuration(leaseDuration), 
                                   leaseTable(h), handler);
    
    final UUID c = UUID.randomUUID();
    final long beforeElection = System.currentTimeMillis();
    e.getRegistry().enrol("resource", c);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c)));
    assertEquals(1, e.getLeaseView().asMap().size());
    await.until(() -> verify(handler).onAssign(eq("resource"), eq(c)));
    final Lease lease = e.getLeaseView().asMap().get("resource");
    assertEquals(c, lease.getTenant());
    assertTrue(lease.getExpiry() >= beforeElection + leaseDuration);
  }
  
  /**
   *  Tests the touching of a lease by a consumer that doesn't hold the lease and, in fact, the tenancy
   *  is vacant.
   *  
   *  @throws NotTenantException
   */
  @Test(expected=NotTenantException.class)
  public void testSingleNodeTouchNotTenantVacant() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);

    final UUID c = UUID.randomUUID();
    e.touch("resource", c);
  }

  /**
   *  Tests the touching of a lease by a consumer that doesn't hold the lease, which is held by another
   *  tenant.
   *  
   *  @throws NotTenantException
   */
  @Test(expected=NotTenantException.class)
  public void testSingleNodeTouchNotTenantOther() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);
    
    final UUID c0 = UUID.randomUUID();
    e.getRegistry().enrol("resource", c0);
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c0)));
    assertEquals(1, e.getLeaseView().asMap().size());

    final UUID c1 = UUID.randomUUID();
    e.touch("resource", c1);
  }

  /**
   *  Simulates a race condition where a tenant holding a lease attempts to touch it, but the lease
   *  is transferred to another tenant behind the scenes. This tests the CAS operation that guards
   *  against the race condition.
   *  
   *  @throws NotTenantException
   */
  @Test(expected=NotTenantException.class)
  public void testSingleNodeTouchNotTenantBackgroundReElection() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    
    // keep a long scavenge interval to desensitise the scavenger and pre-register candidate to ensure that
    // it's the first thing that the scavenger thread sees
    final int scavengeInterval = 30_000;
    final UUID c0 = UUID.randomUUID();
    final Registry initialRegistry = new Registry();
    initialRegistry.enrol("resource", c0);
    
    final Election e = newElection(new ElectionConfig()
                                   .withScavengeInterval(scavengeInterval)
                                   .withInitialRegistry(initialRegistry), 
                                   leaseTable(h), 
                                   handler);
    
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c0)));
    assertEquals(1, e.getLeaseView().asMap().size());

    final UUID c1 = UUID.randomUUID();
    leaseTable(h).put("resource", Lease.forever(c1).pack());
    
    // according to the view snapshot, c0 is the leader, but behind the scenes we've elected c1
    assertTrue(e.getLeaseView().isCurrentTenant("resource", c0));
    e.touch("resource", c0);
  }

  /**
   *  Tests the yield of a lease by a consumer that doesn't hold the lease and, in fact, the tenancy
   *  is vacant.
   *  
   *  @throws NotTenantException
   */
  @Test(expected=NotTenantException.class)
  public void testSingleNodeYieldNotTenantVacant() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    final Election e = newElection(new ElectionConfig().withScavengeInterval(1), leaseTable(h), handler);

    final UUID c = UUID.randomUUID();
    e.yield("resource", c);
  }
  
  /**
   *  Simulates a race condition where a tenant holding a lease attempts to yield it, but the lease
   *  is transferred to another tenant behind the scenes. This tests the CAS operation that guards
   *  against the race condition.
   *  
   *  @throws NotTenantException
   */
  @Test(expected=NotTenantException.class)
  public void testSingleNodeYieldNotTenantBackgroundReElection() throws NotTenantException {
    final HazelcastInstance h = newInstance();
    final ScavengeWatcher handler = mockHandler();
    
    // keep a long scavenge interval to desensitise the scavenger and pre-register candidate to ensure that
    // it's the first thing that the scavenger thread sees
    final int scavengeInterval = 30_000;
    final UUID c0 = UUID.randomUUID();
    final Registry initialRegistry = new Registry();
    initialRegistry.enrol("resource", c0);
    
    final Election e = newElection(new ElectionConfig()
                                   .withScavengeInterval(scavengeInterval)
                                   .withInitialRegistry(initialRegistry), 
                                   leaseTable(h), 
                                   handler);
    
    await.until(() -> assertTrue(e.getLeaseView().isCurrentTenant("resource", c0)));
    assertEquals(1, e.getLeaseView().asMap().size());

    final UUID c1 = UUID.randomUUID();
    leaseTable(h).put("resource", Lease.forever(c1).pack());
    
    // according to the view snapshot, c0 is the leader, but behind the scenes we've elected c1
    assertTrue(e.getLeaseView().isCurrentTenant("resource", c0));
    e.yield("resource", c0);
  }
}
