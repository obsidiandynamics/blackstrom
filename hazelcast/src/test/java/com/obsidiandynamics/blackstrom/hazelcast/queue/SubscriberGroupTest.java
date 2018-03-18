package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class SubscriberGroupTest extends AbstractPubSubTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  /**
   *  Seek can only be performed in a group-free context.
   */
  @Test(expected=IllegalStateException.class)
  public void testIllegalSeek() {
    final String stream = "s";
    final String group = "g";
    final int capacity = 1;

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s =
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    s.seek(0);
    verifyNoError(eh);
  }

  /**
   *  Consuming from an empty buffer should result in a zero-size batch.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testConsumeEmpty() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s = 
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh)
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
    
    verifyNoError(eh);
  }

  /**
   *  Tests consuming of messages, and that {@code Subscriber#poll(long)} extends
   *  the lease in the background. Also checks that we can confirm the offset of
   *  the last consumed message.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testConsumeExtendLeaseAndConfirm() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;

    final HazelcastInstance instance = newInstance();
    
    // start with an expired lease -- the new subscriber should take over it
    final IMap<String, byte[]> leaseTable = instance.getMap(QNamespace.HAZELQ_META.qualify("lease." + stream));
    leaseTable.put(group, Lease.expired(UUID.randomUUID()).pack());

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s = 
        configureSubscriber(instance,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = s.getInstance().getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = s.getInstance().getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    
    // start with the earliest offset
    offsets.put(group, -1L);
    
    // publish a pair of messages and wait until the subscriber is elected
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    await.untilTrue(s::isAssigned);
    
    // capture the original lease expiry so that it can be compared with the extended lease
    final long expiry0 = s.getElection().getLeaseView().getLease(group).getExpiry();
    
    // sleep and then consume messages -- this will eventually extend the lease
    final long sleepTime = 10;
    Thread.sleep(sleepTime);
    final RecordBatch b0 = s.poll(1_000);
    assertEquals(2, b0.size());
    assertArrayEquals("h0".getBytes(), b0.all().get(0).getData());
    assertArrayEquals("h1".getBytes(), b0.all().get(1).getData());
    
    // confirm offsets and check in the map
    s.confirm();
    await.until(() -> assertEquals(1, (long) offsets.get(group)));
    
    // polling would also have extended the lease -- check the expiry
    await.until(() -> {
      final long expiry1 = s.getElection().getLeaseView().getLease(group).getExpiry();
      assertTrue("expiry0=" + expiry0 + ", expiry1=" + expiry1, expiry1 >= expiry0 + sleepTime);
    });
    
    verifyNoError(eh);
  }
  
  /**
   *  Test confirmation with an illegal offset -- below the minimum allowed.
   */
  @Test(expected=IllegalArgumentException.class)
  public void testConfirmFailureOffsetTooLow() {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s = 
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    
    s.confirm(-1);
    verifyNoError(eh);
  }
  
  /**
   *  Test confirmation with an illegal offset -- above the last read offset.
   */
  @Test(expected=IllegalArgumentException.class)
  public void testConfirmFailureOffsetTooHigh() {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s = 
        configureSubscriber(new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    
    s.confirm(0);
    verifyNoError(eh);
  }
  
  /**
   *  Tests the failure of a deactivation when the subscriber isn't the current tenant.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testConfirmAndDeactivateFailureNotAssigned() throws InterruptedException {
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
    
    // try deactivating (can only happen for a current tenant) and verify failure
    s.deactivate();
    await.until(() -> verify(errorHandler).onError(isNotNull(), isNull()));
  }
  
  /**
   *  Tests the failure of a read by creating a buffer overflow condition, where the subscriber
   *  is about to read from an offset that has already lapsed, throwing a {@link StaleSequenceException}
   *  in the background. (The store factory has to be disabled for this to happen.)<p>
   *  
   *  When this error is detected, this test is also rigged to evict the subscriber's tenancy from the
   *  lease table, thereby creating a second error when a lease extension is attempted.
   *  
   *  @throws InterruptedException
   */
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
  
  /**
   *  Tests the {@link InitialOffsetScheme#EARLIEST} offset initialisation.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testInitialOffsetEarliest() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    final HazelcastInstance instance = newInstance();
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s = 
        configureSubscriber(instance,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh)
                            .withInitialOffsetScheme(InitialOffsetScheme.EARLIEST)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    await.untilTrue(s::isAssigned);
    
    final RecordBatch b = s.poll(1_000);
    assertEquals(2, b.size());
    
    verifyNoError(eh);
  }
  
  /**
   *  Tests the {@link InitialOffsetScheme#LATEST} offset initialisation.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testInitialOffsetLatest() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    final HazelcastInstance instance = newInstance();
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());

    final ErrorHandler eh = mockErrorHandler();
    final DefaultSubscriber s = 
        configureSubscriber(instance,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh)
                            .withInitialOffsetScheme(InitialOffsetScheme.LATEST)
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    await.untilTrue(s::isAssigned);
    
    final RecordBatch b = s.poll(10);
    assertEquals(0, b.size());
    
    verifyNoError(eh);
  }
  
  /**
   *  Tests the {@link InitialOffsetScheme#NONE} offset initialisation. Because no offset
   *  has been written to the offsets map, this operation will fail.
   *  
   *  @throws InterruptedException
   */
  @Test(expected=OffsetLoadException.class)
  public void testInitialOffsetNone() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;
    
    final ErrorHandler eh = mockErrorHandler();
    configureSubscriber(new SubscriberConfig()
                        .withGroup(group)
                        .withInitialOffsetScheme(InitialOffsetScheme.NONE)
                        .withErrorHandler(eh)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    verifyNoError(eh);
  }

  /**
   *  Tests two subscribers competing for the same stream. Deactivation and reactivation is used
   *  to test lease reassignment and verify that one subscriber can continue where the other has
   *  left off.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testTwoSubscribersWithActivation() throws InterruptedException {
    final String stream = "s";
    final String group = "g";
    final int capacity = 10;

    final HazelcastInstance instance0 = newInstance();
    final HazelcastInstance instance1 = newInstance();
    final Ringbuffer<byte[]> buffer = instance0.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = instance0.getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group, -1L);
    
    final ErrorHandler eh0 = mockErrorHandler();
    final DefaultSubscriber s0 = 
        configureSubscriber(instance0,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh0)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    await.untilTrue(s0::isAssigned);
    
    final ErrorHandler eh1 = mockErrorHandler();
    final DefaultSubscriber s1 = 
        configureSubscriber(instance1,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh1)
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
    
    // consume from s1 -- should return an empty batch
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
    verifyNoError(eh0, eh1);
  }
  
  /**
   *  Tests two subscribers using two separate groups. Each subscriber will receive the
   *  same messages.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testTwoSubscribersTwoGroups() throws InterruptedException {
    final String stream = "s";
    final String group0 = "g0";
    final String group1 = "g1";
    final int capacity = 10;

    final HazelcastInstance instance0 = newInstance();
    final HazelcastInstance instance1 = newInstance();
    final Ringbuffer<byte[]> buffer = instance0.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final IMap<String, Long> offsets = instance0.getMap(QNamespace.HAZELQ_META.qualify("offsets." + stream));
    offsets.put(group0, -1L);
    offsets.put(group1, -1L);
    
    final ErrorHandler eh0 = mockErrorHandler();
    final DefaultSubscriber s0 = 
        configureSubscriber(instance0,
                            new SubscriberConfig()
                            .withGroup(group0)
                            .withErrorHandler(eh0)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    final ErrorHandler eh1 = mockErrorHandler();
    final DefaultSubscriber s1 = 
        configureSubscriber(instance1,
                            new SubscriberConfig()
                            .withGroup(group1)
                            .withErrorHandler(eh1)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream)
                                              .withHeapCapacity(capacity)));
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    await.untilTrue(s0::isAssigned);
    await.untilTrue(s1::isAssigned);

    // consume from s0 and s1, and verify that both received the same messages
    final RecordBatch s0_b0 = s0.poll(1_000);
    assertEquals(2, s0_b0.size());
    assertArrayEquals("h0".getBytes(), s0_b0.all().get(0).getData());
    assertArrayEquals("h1".getBytes(), s0_b0.all().get(1).getData());

    final RecordBatch s1_b0 = s1.poll(1_000);
    assertEquals(2, s1_b0.size());
    assertArrayEquals("h0".getBytes(), s1_b0.all().get(0).getData());
    assertArrayEquals("h1".getBytes(), s1_b0.all().get(1).getData());
    
    // consume again from s0 and s1 should result in an empty batch
    final RecordBatch s0_b1 = s0.poll(10);
    assertEquals(0, s0_b1.size());
    
    final RecordBatch s1_b1 = s1.poll(10);
    assertEquals(0, s1_b1.size());
    
    verifyNoError(eh0, eh1);
  }
  
  /**
   *  Tests two subscribers with two separate streams but using identical group IDs. Each subscriber will 
   *  receive messages from its own stream -- there are no interactions among groups for different streams.
   *  
   *  @throws InterruptedException
   */
  @Test
  public void testTwoSubscribersTwoStreams() throws InterruptedException {
    final String stream0 = "s";
    final String stream1 = "s1";
    final String group = "g";
    final int capacity = 10;

    final HazelcastInstance instance0 = newInstance();
    final HazelcastInstance instance1 = newInstance();
    final Ringbuffer<byte[]> buffer0 = instance0.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream0));
    final Ringbuffer<byte[]> buffer1 = instance1.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream1));
    
    final ErrorHandler eh0 = mockErrorHandler();
    final DefaultSubscriber s0 = 
        configureSubscriber(instance0,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh0)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream0)
                                              .withHeapCapacity(capacity)));

    final ErrorHandler eh1 = mockErrorHandler();
    final DefaultSubscriber s1 = 
        configureSubscriber(instance1,
                            new SubscriberConfig()
                            .withGroup(group)
                            .withErrorHandler(eh1)
                            .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
                            .withStreamConfig(new StreamConfig()
                                              .withName(stream1)
                                              .withHeapCapacity(capacity)));
    // both subscribers should assume leadership -- each for its own stream
    await.untilTrue(s0::isAssigned);
    await.untilTrue(s1::isAssigned);

    // publish to s0's stream buffer
    buffer0.add("s0h0".getBytes());
    buffer0.add("s0h1".getBytes());
    
    // consume from s0
    final RecordBatch s0_b0 = s0.poll(1_000);
    assertEquals(2, s0_b0.size());
    assertArrayEquals("s0h0".getBytes(), s0_b0.all().get(0).getData());
    assertArrayEquals("s0h1".getBytes(), s0_b0.all().get(1).getData());
    
    // consumer again from s0 -- should get an empty batch
    final RecordBatch s0_b1 = s0.poll(10);
    assertEquals(0, s0_b1.size());
    
    // publish to s1's stream buffer
    buffer1.add("s1h0".getBytes());
    buffer1.add("s1h1".getBytes());

    // consume from s1
    final RecordBatch s1_b0 = s1.poll(1_000);
    assertEquals(2, s1_b0.size());
    assertArrayEquals("s1h0".getBytes(), s1_b0.all().get(0).getData());
    assertArrayEquals("s1h1".getBytes(), s1_b0.all().get(1).getData());
    
    // consumer again from s1 -- should get an empty batch
    final RecordBatch s1_b1 = s1.poll(10);
    assertEquals(0, s1_b1.size());
    
    verifyNoError(eh0, eh1);
  }
}