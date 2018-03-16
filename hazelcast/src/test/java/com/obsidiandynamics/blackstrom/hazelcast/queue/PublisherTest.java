package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import org.junit.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class PublisherTest {
  private HazelcastProvider provider;
  
  private HazelcastInstance instance;
  
  private Publisher publisher;
  
  private final Timesert await = Wait.SHORT;
  
  @Before
  public void before() {
    provider = new MockHazelcastProvider();
    instance = provider.createInstance(new Config().setProperty("hazelcast.logging.type", "none"));
  }
  
  @After
  public void after() {
    if (publisher != null) publisher.terminate().joinQuietly();
    if (instance != null) instance.shutdown();
  }
  
  private void configurePublisher(PublisherConfig config) {
    publisher = Publisher.createDefault(instance, config);
  }
  
  private static class TestCallback implements PublishCallback {
    long offset = Record.UNASSIGNED_OFFSET;
    Throwable error;
    
    @Override
    public void onComplete(long offset, Throwable error) {
      this.offset = offset;
      this.error = error;
    }
    
    boolean isComplete() {
      return offset != Record.UNASSIGNED_OFFSET || error != null;
    }
  }
  
  @Test
  public void testPublishToBoundedBuffer() throws InterruptedException, ExecutionException {
    final String stream = "s";
    final int capacity = 10;
    
    configurePublisher(new PublisherConfig().withStreamConfig(new StreamConfig()
                                                              .withName(stream)
                                                              .withHeapCapacity(capacity)));
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final List<TestCallback> callbacks = new ArrayList<>();
    
    final int initialMessages = 5;
    publish(initialMessages, publisher, callbacks);
    
    await.until(() -> assertEquals(initialMessages, completed(callbacks).size()));
    assertEquals(initialMessages, buffer.size());
    final List<byte[]> initialItems = readRemaining(buffer, 0);
    assertEquals(initialMessages, initialItems.size());
    
    final int furtherMessages = 20;
    publish(furtherMessages, publisher, callbacks);
    
    await.until(() -> assertEquals(initialMessages + furtherMessages, completed(callbacks).size()));
    assertEquals(capacity, buffer.size());
    final List<byte[]> allItems = readRemaining(buffer, 15);
    assertEquals(capacity, allItems.size());
  }
  
  @Test
  public void testPublishToStoredBuffer() throws InterruptedException, ExecutionException {
    final String stream = "s";
    final int capacity = 10;
    
    configurePublisher(new PublisherConfig().withStreamConfig(new StreamConfig()
                                                              .withName(stream)
                                                              .withHeapCapacity(capacity)
                                                              .withResidualStoreFactory(new HeapRingbufferStore.Factory())));
    final Ringbuffer<byte[]> buffer = instance.getRingbuffer(QNamespace.HAZELQ_STREAM.qualify(stream));
    final List<TestCallback> callbacks = new ArrayList<>();
    
    final int initialMessages = 5;
    publish(initialMessages, publisher, callbacks);
    
    await.until(() -> assertEquals(initialMessages, completed(callbacks).size()));
    assertEquals(initialMessages, buffer.size());
    final List<byte[]> initialItems = readRemaining(buffer, 0);
    assertEquals(initialMessages, initialItems.size());
    
    final int furtherMessages = 20;
    publish(furtherMessages, publisher, callbacks);
    
    await.until(() -> assertEquals(initialMessages + furtherMessages, completed(callbacks).size()));
    assertEquals(capacity, buffer.size());
    final List<byte[]> allItems = readRemaining(buffer, 0);
    assertEquals(initialMessages + furtherMessages, allItems.size());
  }
  
  @Test
  public void testPublishFailure() {
    
  }
  
  private static void publish(int numMessages, Publisher publisher, List<TestCallback> callbacks) {
    for (int i = 0; i < numMessages; i++) {
      final TestCallback callback = new TestCallback();
      callbacks.add(callback);
      publisher.publishAsync(new Record("hello".getBytes()), callback);
    }
  }
  
  private static List<byte[]> readRemaining(Ringbuffer<byte[]> buffer, long startSequence) throws InterruptedException, ExecutionException {
    final ReadResultSet<byte[]> results = buffer
        .readManyAsync(startSequence, 0, 1000, null)
        .get();
    final List<byte[]> items = new ArrayList<>(results.size());
    results.forEach(items::add);
    return items;
  }
  
  private static List<TestCallback> completed(List<TestCallback> callbacks) {
    return callbacks.stream().filter(c -> c.isComplete()).collect(Collectors.toList());
  }
}