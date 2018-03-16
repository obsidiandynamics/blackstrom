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
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class SubscriberNoGroupTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private HazelcastProvider provider;
  
  private HazelcastInstance instance;
  
  private DefaultSubscriber subscriber;
  
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
    final int capacity = 10;
    
    configureSubscriber(new SubscriberConfig().withStreamConfig(new StreamConfig()
                                                                .withName(stream)
                                                                .withHeapCapacity(capacity)));
    final RecordBatch b0 = subscriber.poll(1);
    assertEquals(0, b0.size());
    
    final RecordBatch b1 = subscriber.poll(1);
    assertEquals(0, b1.size());
    subscriber.confirm(); // shouldn't do anything
    
    assertTrue(subscriber.isAssigned()); // should always be true in group-free mode
  }
  
  @Test
  public void testConsumeOne() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;
    
    configureSubscriber(new SubscriberConfig().withStreamConfig(new StreamConfig()
                                                                .withName(stream)
                                                                .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    
    buffer.add("hello".getBytes());
    
    final RecordBatch b0 = subscriber.poll(1_000);
    assertEquals(1, b0.size());
    assertArrayEquals("hello".getBytes(), b0.all().get(0).getData());
    
    final RecordBatch b1 = subscriber.poll(10);
    assertEquals(0, b1.size());
    subscriber.confirm(); // shouldn't do anything
  }
  
  @Test
  public void testConsumeTwo() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;
    
    configureSubscriber(new SubscriberConfig().withStreamConfig(new StreamConfig()
                                                                .withName(stream)
                                                                .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    
    final RecordBatch b0 = subscriber.poll(1_000);
    assertEquals(2, b0.size());
    assertArrayEquals("h0".getBytes(), b0.all().get(0).getData());
    assertArrayEquals("h1".getBytes(), b0.all().get(1).getData());
    
    final RecordBatch b1 = subscriber.poll(10);
    assertEquals(0, b1.size());
  }
  
  @Test
  public void testSeek() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;
    
    configureSubscriber(new SubscriberConfig().withStreamConfig(new StreamConfig()
                                                                .withName(stream)
                                                                .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    
    subscriber.seek(1);
    final RecordBatch b0 = subscriber.poll(1_000);
    assertEquals(1, b0.size());
    assertArrayEquals("h1".getBytes(), b0.all().get(0).getData());
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testSeekIllegalArgumentTooLow() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;
    
    configureSubscriber(new SubscriberConfig().withStreamConfig(new StreamConfig()
                                                                .withName(stream)
                                                                .withHeapCapacity(capacity)));
    subscriber.seek(-5);
  }
  
  @Test
  public void testReadFailure() throws InterruptedException {
    final String stream = "s";
    final int capacity = 1;
    final ErrorHandler errorHandler = mock(ErrorHandler.class);
    
    configureSubscriber(new SubscriberConfig()
                        .withErrorHandler(errorHandler)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)
                                          .withResidualStoreFactory(null)));
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    final RecordBatch b = subscriber.poll(1_000);
    assertEquals(0, b.size());
    verify(errorHandler).onError(isNotNull(), (Exception) isNotNull());
  }
  
  @Test
  public void testInitialOffsetEarliest() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;
    
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    
    configureSubscriber(new SubscriberConfig()
                        .withInitialOffsetScheme(InitialOffsetScheme.EARLIEST)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    
    final RecordBatch b = subscriber.poll(1_000);
    assertEquals(2, b.size());
  }
  
  @Test
  public void testInitialOffsetLatest() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;
    
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    buffer.add("h0".getBytes());
    buffer.add("h1".getBytes());
    
    configureSubscriber(new SubscriberConfig()
                        .withInitialOffsetScheme(InitialOffsetScheme.LATEST)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
    
    final RecordBatch b = subscriber.poll(10);
    assertEquals(0, b.size());
  }
  
  @Test(expected=InvalidInitialOffsetSchemeException.class)
  public void testInitialOffsetNone() throws InterruptedException {
    final String stream = "s";
    final int capacity = 10;
    
    configureSubscriber(new SubscriberConfig()
                        .withInitialOffsetScheme(InitialOffsetScheme.NONE)
                        .withStreamConfig(new StreamConfig()
                                          .withName(stream)
                                          .withHeapCapacity(capacity)));
  }
}