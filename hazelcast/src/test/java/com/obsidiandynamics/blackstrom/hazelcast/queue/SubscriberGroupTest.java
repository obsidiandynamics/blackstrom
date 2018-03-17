package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class SubscriberGroupTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private HazelcastProvider defaultProvider;
  
  private final Set<HazelcastInstance> instances = new HashSet<>();
  
  private final Set<DefaultSubscriber> subscribers = new HashSet<>();
  
  private final Timesert await = Wait.SHORT;
  
  @Before
  public void before() {
    defaultProvider = new MockHazelcastProvider();
  }
  
  @After
  public void after() {
    subscribers.forEach(s -> s.terminate());
    subscribers.forEach(s -> s.joinQuietly());
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
  
  private DefaultSubscriber configureSubscriber(SubscriberConfig config) {
    return configureSubscriber(newInstance(), config);
  }
  
  private DefaultSubscriber configureSubscriber(HazelcastInstance instance, SubscriberConfig config) {
    final DefaultSubscriber subscriber = Subscriber.createDefault(instance, config);
    subscribers.add(subscriber);
    return subscriber;
  }

  @Test
  public void testConsumeEmpty() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    final DefaultSubscriber s = 
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final IMap<String, Long> offsets = s.getInstance().getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));

    final RecordBatch b0 = s.poll(1);
    assertEquals(0, b0.size());
    
    await.untilTrue(s::isAssigned);
    final RecordBatch b1 = s.poll(1);
    assertEquals(0, b1.size());
    
    assertEquals(0, offsets.size());
    s.confirm();
  }

  @Test
  public void testConsumeConfirmAndUpdateLease() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    final DefaultSubscriber s = 
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = s.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = s.getInstance().getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group, -1L);
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    await.untilTrue(s::isAssigned);
    final long expiry0 = s.getElection().getLeaseView().getLease(group).getExpiry();
    
    Thread.sleep(10);
    final RecordBatch b0 = s.poll(1_000);
    assertEquals(2, b0.size());
    assertArrayEquals("h0".getBytes(), b0.all().get(0).getData());
    assertArrayEquals("h1".getBytes(), b0.all().get(1).getData());
    
    // confirm offsets and check in the map
    s.confirm();
    await.until(() -> assertEquals(1, (long) offsets.get(group)));
    
    // polling would also have touched the lease -- check the expiry
    await.until(() -> {
      final long expiry1 = s.getElection().getLeaseView().getLease(group).getExpiry();
      assertTrue("expiry0=" + expiry0 + ", expiry1=" + expiry1, expiry1 > expiry0);
    });
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testConfirmFailureOffsetTooLow() {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    final DefaultSubscriber s = 
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    
    s.confirm(-1);
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testConfirmFailureOffsetTooHigh() {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    final DefaultSubscriber s = 
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    
    s.confirm(0);
  }
  
  @Test
  public void testConfirmFailureNotAssigned() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    final ErrorHandler errorHandler = mock(ErrorHandler.class);
    final DefaultSubscriber s = 
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(errorHandler)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final IMap<String, byte[]> leaseTable = s.getInstance().getMap(QNamespace.HAZELQ_META.qualify("lease." + stream));
    final Ringbuffer<byte[]> buffer = s.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = s.getInstance().getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group, -1L);
    
    // write some data so that we can read at least one record (otherwise we can't confirm an offset)
    buffer.add("hello".getBytes());
    await.untilTrue(s::isAssigned);
    final long expiry0 = s.getElection().getLeaseView().getLease(group).getExpiry();

    Thread.sleep(10);
    final RecordBatch b0 = s.poll(1_000);
    assertEquals(1, b0.size());
    
    // wait until the subscriber has touched its lease
    await.until(() -> {
      final long expiry1 = s.getElection().getLeaseView().getLease(group).getExpiry();
      assertTrue("expiry0=" + expiry0 + ", expiry1=" + expiry1, expiry1 > expiry0);
    });
    
    // forcibly take the lease away and confirm that the subscriber has seen this 
    leaseTable.put(group, Lease.forever(UUID.randomUUID()).pack());
    await.until(() -> assertFalse(s.isAssigned()));
    
    // schedule a confirmation and verify that it has failed
    s.confirm();
    await.until(() -> verify(errorHandler).onError(isNotNull(), isNull()));
  }
  
  @Test
  public void testPollReadFailureAndExtendFailure() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 1;
    
    final ErrorHandler errorHandler = mock(ErrorHandler.class);
    final DefaultSubscriber s = 
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(errorHandler)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withStoreFactoryClass(null)
                                              .withHeapCapacity(capacity)));
    final IMap<String, byte[]> leaseTable = s.getInstance().getMap(QNamespace.HAZELQ_META.qualify("lease." + stream));
    final Ringbuffer<byte[]> buffer = s.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = s.getInstance().getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group, -1L);
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    await.untilTrue(s::isAssigned);
    
    // when an error occurs, forcibly reassign the lease
    doAnswer(invocation -> leaseTable.put(group, Lease.forever(UUID.randomUUID()).pack()))
    .when(errorHandler).onError(any(), any());
    
    // simulate an error by reading stale items
    Thread.sleep(10);
    final RecordBatch b = s.poll(1_000);
    assertEquals(0, b.size());
    
    // there should be two errors -- the first for the stale read, the second for the failed lease extension
    await.until(() -> verify(errorHandler, times(2)).onError(isNotNull(), (Exception) isNotNull()));
  }
  
  @Test
  public void testInitialOffsetEarliest() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    final HazelcastInstance instance = newInstance();
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    
    final DefaultSubscriber s = 
        configureSubscriber(instance,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withInitialOffsetScheme(InitialOffsetScheme.EARLIEST)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    await.untilTrue(s::isAssigned);
    
    final RecordBatch b = s.poll(1_000);
    assertEquals(2, b.size());
  }
  
  @Test
  public void testInitialOffsetLatest() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    final HazelcastInstance instance = newInstance();
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());

    final DefaultSubscriber s = 
        configureSubscriber(instance,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withInitialOffsetScheme(InitialOffsetScheme.LATEST)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    await.untilTrue(s::isAssigned);
    
    final RecordBatch b = s.poll(10);
    assertEquals(0, b.size());
  }
  
  @Test(expected=OffsetInitializationException.class)
  public void testInitialOffsetNone() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withInitialOffsetScheme(InitialOffsetScheme.NONE)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
  }

  @Test
  public void testTwoConsumersWithActivation() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;

    final HazelcastInstance instance = newInstance();
    final DefaultSubscriber s0 = 
        configureSubscriber(instance,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = s0.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = s0.getInstance().getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group, -1L);
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    await.untilTrue(s0::isAssigned);
    
    final DefaultSubscriber s1 = 
        configureSubscriber(instance,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    Thread.sleep(10);
    assertFalse(s1.isAssigned()); // s0 is the tenant

    // consume from s0 -- should succeed
    final RecordBatch s0_b0 = s0.poll(1_000);
    assertEquals(2, s0_b0.size());
    assertArrayEquals("h0".getBytes(), s0_b0.all().get(0).getData());
    assertArrayEquals("h1".getBytes(), s0_b0.all().get(1).getData());
    
    // confirm offsets and check in the map
    s0.confirm();
    await.until(() -> assertEquals(1, (long) offsets.get(group)));
    
    // consumer from s1 -- should return an empty batch
    final RecordBatch s1_b0 = s0.poll(10);
    assertEquals(0, s1_b0.size());
    
    // publish more records -- should only be available to s0
    buffer.add("h2".getBytes());
    buffer.add("h3".getBytes());

    final RecordBatch s0_b1 = s0.poll(1_000);
    assertEquals(2, s0_b1.size());
    assertArrayEquals("h2".getBytes(), s0_b1.all().get(0).getData());
    assertArrayEquals("h3".getBytes(), s0_b1.all().get(1).getData());

    final RecordBatch s1_b1 = s1.poll(10);
    assertEquals(0, s1_b1.size());
    
    // deactivate s0 -- it will eventually lose the ability to consume messages
    s0.deactivate();
    await.until(() -> assertFalse(s0.isAssigned()));
    final RecordBatch s0_b2 = s0.poll(10);
    assertEquals(0, s0_b2.size());
    
    // s1 will eventually take tenancy
    await.untilTrue(s1::isAssigned);
    
    // consuming now from s1 should fetch the last two records (the first two were already confirmed)
    final RecordBatch s1_b2 = s1.poll(1_000);
    assertEquals(2, s1_b2.size());
    assertArrayEquals("h2".getBytes(), s1_b2.all().get(0).getData());
    assertArrayEquals("h3".getBytes(), s1_b2.all().get(1).getData());
    
    // confirm h2 only and switch tenancy back to s0
    s1.confirm(2);
    await.until(() -> assertEquals(2, (long) offsets.get(group)));
    s1.deactivate();
    s0.reactivate();
    
    // consuming now from s0 should only fetch h3, as h2 was confirmed
    await.until(() -> {
      final RecordBatch s0_b3;
      try {
        s0_b3 = s0.poll(1);
      } catch (InterruptedException e) { return; }
      assertEquals(1, s0_b3.size());
      assertArrayEquals("h3".getBytes(), s0_b3.all().get(0).getData());
    });
    
    await.until(() -> assertFalse(s1.isAssigned()));
  }
}