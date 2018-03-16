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
    return TestCycle.timesQuietly(1000);
  }
  
  private HazelcastProvider provider;
  
  private HazelcastInstance instance;
  
  private DefaultSubscriber subscriber;
  
  private final Timesert await = Wait.SHORT;
  
  @Before
  public void before() {
    provider = new MockHazelcastProvider();
    instance = provider.createInstance(new Config().setProperty("hazelcast.logging.type", "none"));
  }
  
  @After
  public void after() {
    if (subscriber != null) subscriber.terminate().joinQuietly();
    if (instance != null) instance.shutdown();
  }
  
  private void configureSubscriber(SubscriberConfig config) {
    subscriber = Subscriber.createDefault(instance, config);
  }

  @Test
  public void testConsumeEmpty() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    final IMap<String, Long> offsets = instance.getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));

    final RecordBatch b0 = subscriber.poll(1);
    assertEquals(0, b0.size());
    
    await.untilTrue(subscriber::isAssigned);
    final RecordBatch b1 = subscriber.poll(1);
    assertEquals(0, b1.size());
    
    assertEquals(0, offsets.size());
    subscriber.confirm();
  }

  @Test
  public void testConsumeConfirmAndUpdateLease() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = instance.getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group, -1L);
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    await.untilTrue(subscriber::isAssigned);
    final long expiry0 = subscriber.getElection().getLeaseView().getLease(group).getExpiry();
    
    Thread.sleep(10);
    final RecordBatch b0 = subscriber.poll(1_000);
    assertEquals(2, b0.size());
    assertArrayEquals("h0".getBytes(), b0.all().get(0).getData());
    assertArrayEquals("h1".getBytes(), b0.all().get(1).getData());
    
    // confirm offsets and check in the map
    subscriber.confirm();
    await.until(() -> assertEquals(1, (long) offsets.get(group)));
    
    // polling would also have touched the lease -- check the expiry
    await.until(() -> {
      final long expiry1 = subscriber.getElection().getLeaseView().getLease(group).getExpiry();
      assertTrue("expiry0=" + expiry0 + ", expiry1=" + expiry1, expiry1 > expiry0);
    });
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testConfirmFailureOffsetTooLow() {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    
    subscriber.confirm(-1);
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testConfirmFailureOffsetTooHigh() {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    
    subscriber.confirm(0);
  }
  
  @Test
  public void testConfirmFailureNotAssigned() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    final ErrorHandler errorHandler = mock(ErrorHandler.class);
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withErrorHandler(errorHandler)
                        .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    final IMap<String, byte[]> leaseTable = instance.getMap(QNamespace.HAZELQ_META.qualify("lease." + stream));
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = instance.getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group, -1L);
    
    // write some data so that we can read at least one record (otherwise we can't confirm an offset)
    buffer.add("hello".getBytes());
    await.untilTrue(subscriber::isAssigned);
    final long expiry0 = subscriber.getElection().getLeaseView().getLease(group).getExpiry();

    Thread.sleep(10);
    final RecordBatch b0 = subscriber.poll(1_000);
    assertEquals(1, b0.size());
    
    // wait until the subscriber has touched its lease
    await.until(() -> {
      final long expiry1 = subscriber.getElection().getLeaseView().getLease(group).getExpiry();
      assertTrue("expiry0=" + expiry0 + ", expiry1=" + expiry1, expiry1 > expiry0);
    });
    
    // forcibly take the lease away and confirm that the subscriber has seen this 
    leaseTable.put(group, Lease.forever(UUID.randomUUID()).pack());
    await.until(() -> assertFalse(subscriber.isAssigned()));
    
    // schedule a confirmation and verify that it has failed
    subscriber.confirm();
    await.until(() -> verify(errorHandler).onError(isNotNull(), isNull()));
  }
  
  @Test
  public void testPollReadFailureAndExtendFailure() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 1;
    
    final ErrorHandler errorHandler = mock(ErrorHandler.class);
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withErrorHandler(errorHandler)
                        .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withResidualStoreFactory(null)
                                          .withHeapCapacity(capacity)));
    final IMap<String, byte[]> leaseTable = instance.getMap(QNamespace.HAZELQ_META.qualify("lease." + stream));
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = instance.getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group, -1L);
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    await.untilTrue(subscriber::isAssigned);
    
    // when an error occurs, forcibly reassign the lease
    doAnswer(invocation -> leaseTable.put(group, Lease.forever(UUID.randomUUID()).pack()))
    .when(errorHandler).onError(any(), any());
    
    // simulate an error by reading stale items
    Thread.sleep(10);
    final RecordBatch b = subscriber.poll(1_000);
    assertEquals(0, b.size());
    
    // there should be two errors -- the first for the stale read, the second for the failed lease extension
    await.until(() -> verify(errorHandler, times(2)).onError(isNotNull(), (Exception) isNotNull()));
  }
  
  @Test
  public void testInitialOffsetEarliest() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withInitialOffsetScheme(InitialOffsetScheme.EARLIEST)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    await.untilTrue(subscriber::isAssigned);
    
    final RecordBatch b = subscriber.poll(1_000);
    assertEquals(2, b.size());
  }
  
  @Test
  public void testInitialOffsetLatest() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withInitialOffsetScheme(InitialOffsetScheme.LATEST)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    await.untilTrue(subscriber::isAssigned);
    
    final RecordBatch b = subscriber.poll(10);
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
}