package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.*;
import org.mockito.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class SubscriberGroupTest {
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
  public void testConsumeEmptyResetOffset() throws InterruptedException {
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
    final IMap<String, Long> offsets = instance.getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group, -1L);
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    
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
    await.until(() -> assertEquals(1, offsets.size()));
    assertEquals(1, (long) offsets.get(group));
    
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
    leaseTable.put(group, new Lease(UUID.randomUUID(), Long.MAX_VALUE).pack());
    await.until(() -> assertFalse(subscriber.isAssigned()));
    
    // schedule a confirmation and verify that it has failed
    subscriber.confirm();
    await.until(() -> verify(errorHandler).onError(isNotNull(), isNull()));
  }
}