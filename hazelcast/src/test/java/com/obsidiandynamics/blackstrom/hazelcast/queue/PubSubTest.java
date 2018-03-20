package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.Receiver.*;
import com.obsidiandynamics.blackstrom.hazelcast.util.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class PubSubTest extends AbstractPubSubTest {
  private final int SCALE = Testmark.getOptions(Scale.class, Scale.UNITY).magnitude();
  
  private static class TestMessage {
    final long id;

    TestMessage(long id) { this.id = id; }
    
    byte[] pack() {
      final ByteBuffer buf = ByteBuffer.allocate(8);
      buf.putLong(id);
      return buf.array();
    }
    
    static TestMessage unpack(byte[] bytes) {
      return new TestMessage(ByteBuffer.wrap(bytes).getLong());
    }

    @Override
    public String toString() {
      return TestMessage.class.getSimpleName() + " [id=" + id + "]";
    }
  }
  
  private static class TestHandler implements RecordHandler {
    private final List<TestMessage> received = new CopyOnWriteArrayList<>();
    
    private volatile long lastId = -1;
    private volatile AssertionError error;

    @Override
    public void onRecord(Record record) {
      final TestMessage message = TestMessage.unpack(record.getData());
      final long id = message.id;
      if (lastId == -1) {
        lastId = id;
      } else {
        final long expectedBallotId = lastId + 1;
        if (id != expectedBallotId) {
          error = new AssertionError("Expected ID " + expectedBallotId + ", got " + id);
          throw error;
        } else {
          lastId = id;
        }
      }
      received.add(message);
    }
  }
  
  @Test
  public void testPubSubGroupFree() {
    testPubSub(3, 5, new InstancePool(2, this::newInstance), null);
  }
  
  @Test
  public void testPubSubGroupAware() {
    testPubSub(3, 5, new InstancePool(2, this::newInstance), randomGroup());
  }
  
  private void testPubSub(int numReceivers, int numMessages, InstancePool instancePool, String group) {
    final String stream = "s";
    
    // common configuration
    final StreamConfig streamConfig = new StreamConfig()
        .withName(stream)
        .withHeapCapacity(numMessages);
    
    // prestart the instance pool
    final int prestartInstances = Math.min(1 + numReceivers, instancePool.size());
    instancePool.prestart(prestartInstances);

    // create subscribers with receivers
    final ErrorHandler eh = mockErrorHandler();
    final SubscriberConfig subConfig = new SubscriberConfig()
        .withGroup(group)
        .withErrorHandler(eh)
        .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
        .withStreamConfig(streamConfig);
        
    final List<TestHandler> handlers = new ArrayList<>(numReceivers);
    for (int i = 0; i < numReceivers; i++) {
      final HazelcastInstance instance = instancePool.get();
      final Subscriber s = configureSubscriber(instance, subConfig);
      createReceiver(s, register(new TestHandler(), handlers), 10);
    }
    
    // create a publisher and publish the messages
    final PublisherConfig pubConfig = new PublisherConfig()
        .withStreamConfig(streamConfig);
    final HazelcastInstance instance = instancePool.get();
    final Publisher p = configurePublisher(instance, pubConfig);
    
    final List<FuturePublishCallback> futures = new ArrayList<>(numMessages);
    for (int i = 0; i < numMessages; i++) {
      register(p.publishAsync(new Record(new TestMessage(i).pack())), futures);
    }
    
    // wait until all publish confirmations have been processed
    wait.until(() -> {
      final int completedFutures = (int) futures.stream().filter(f -> f.isDone()).count();
      assertEquals(numMessages, completedFutures);
    });
    
    final int errorredFutures = (int) futures.stream().filter(f -> f.isCompletedExceptionally()).count();
    assertEquals(0, errorredFutures);
    
    // verify received messages; if a failure is detected, deep dive into the contents for debugging
    boolean success = false;
    try {
      wait.until(() -> {
        // list of handlers that have received at least one message
        final List<TestHandler> receivedHandlers = handlers.stream()
            .filter(h -> h.received.size() != 0).collect(Collectors.toList());
        
        // the number of expected receivers depends on whether a group has been set
        if (group != null) {
          assertEquals(1, receivedHandlers.size());
        } else {
          assertEquals(numReceivers, receivedHandlers.size());
        }
        
        for (TestHandler handler : receivedHandlers) {
          assertNull(handler.error);
          assertEquals(numMessages, handler.received.size());
          long index = 0;
          for (TestMessage m  : handler.received) {
            assertEquals(index, m.id);
            index++;
          }
        }
      });
      success = true;
    } finally {
      if (! success) {
        System.out.format("numReceivers=%d, numMessages=%d, instances.size=%d, group=%s\n",
                          numReceivers, numMessages, instancePool.size(), group);
        for (TestHandler handler : handlers) {
          System.out.println("---");
          for (TestMessage m : handler.received) {
            System.out.println("- " + m);
          }
        }
      }
    }
    
    verifyNoError(eh);
  }
  
  @Test
  public void testOneWay() {
    testOneWay(2, 4, 10_000 * SCALE, 10, new InstancePool(2, this::newInstance), new OneWayOptions());
  }
  
  @Test
  public void testOneWayBenchmark() {
    Testmark.ifEnabled("one-way with grid", () -> {
      final OneWayOptions options = new OneWayOptions() {{
        verbose = true;
        printBacklog = false;
      }};
      final Supplier<HazelcastInstance> instanceSupplier = () -> newInstance(GridHazelcastProvider.getInstance());
      final Supplier<InstancePool> poolSupplier = () -> new InstancePool(4, instanceSupplier);
      final int messageSize = 100;
      
      testOneWay(1, 1, 2_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(1, 2, 2_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(1, 4, 2_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(2, 4, 1_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(2, 8, 1_000_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(4, 8, 500_000 * SCALE, messageSize, poolSupplier.get(), options);
      testOneWay(4, 16, 500_000 * SCALE, messageSize, poolSupplier.get(), options);
    });
  }
  
  private static class OneWayOptions {
    boolean verbose;
    boolean printBacklog;
  }
  
  private void testOneWay(int publishers, int subscribers, int messagesPerPublisher, int messageSize, 
                          InstancePool instancePool, OneWayOptions options) {
    final int backlogTarget = 10_000;
    final int checkInterval = backlogTarget;
    final String stream = "s";
    final byte[] message = new byte[messageSize];
    final int capacity = backlogTarget * publishers * 2;
    final int pollTimeoutMillis = 100;
    
    // common configuration
    final StreamConfig streamConfig = new StreamConfig()
        .withName(stream)
        .withHeapCapacity(capacity);
    
    if (options.verbose) System.out.format("Prestarting instances for %d/%d pub/sub... ", publishers, subscribers);
    final int prestartInstances = Math.min(publishers + subscribers, instancePool.size());
    instancePool.prestart(prestartInstances);
    if (options.verbose) System.out.format("ready (x%d). Starting run...\n", prestartInstances);

    // create subscribers with receivers
    final ErrorHandler eh = mockErrorHandler();
    final SubscriberConfig subConfig = new SubscriberConfig()
        .withErrorHandler(eh)
        .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
        .withStreamConfig(streamConfig);
    
    final AtomicLong[] receivedArray = new AtomicLong[subscribers];
    for (int i = 0; i < subscribers; i++) {
      final AtomicLong received = new AtomicLong();
      receivedArray[i] = received;
      
      final HazelcastInstance instance = instancePool.get();
      final Subscriber s = configureSubscriber(instance, subConfig);
      createReceiver(s, record -> received.incrementAndGet(), pollTimeoutMillis);
    }
    
    final LongSupplier totalReceived = () -> {
      long total = 0;
      for (AtomicLong received : receivedArray) {
        total += received.get();
      }
      return total;
    };
    
    final LongSupplier smallestReceived = () -> {
      long smallest = Long.MAX_VALUE;
      for (AtomicLong received : receivedArray) {
        final long r = received.get();
        if (r < smallest) {
          smallest = r;
        }
      }
      return smallest;
    };
    
    // create the publishers and send across several threads
    final PublisherConfig pubConfig = new PublisherConfig()
        .withStreamConfig(streamConfig);
    final List<Publisher> publishersList = IntStream.range(0, publishers).boxed()
        .map(i -> configurePublisher(instancePool.get(), pubConfig)).collect(Collectors.toList());
    
    final AtomicLong totalSent = new AtomicLong();
    final long took = TestSupport.took(() -> {
      ParallelJob.blocking(publishers, threadNo -> {
        final Publisher p = publishersList.get(threadNo);
        
        for (int i = 0; i < messagesPerPublisher; i++) {
          p.publishAsync(new Record(message), PublishCallback.nop());
          
          if (i != 0 && i % checkInterval == 0) {
            long lastLogTime = 0;
            final long sent = totalSent.addAndGet(checkInterval);
            for (;;) {
              final int backlog = (int) (sent - smallestReceived.getAsLong());
              if (backlog >= backlogTarget) {
                TestSupport.sleep(1);
                if (options.printBacklog && System.currentTimeMillis() - lastLogTime > 5_000) {
                  TestSupport.LOG_STREAM.format("throttling... backlog @ %,d (%,d messages)\n", backlog, sent);
                  lastLogTime = System.currentTimeMillis();
                }
              } else {
                break;
              }
            }
          }
        }
      }).run();

      wait.until(() -> {
        assertEquals(publishers * messagesPerPublisher * (long) subscribers, totalReceived.getAsLong());
      });
    });
                                     
    final long totalMessages = (long) publishers * messagesPerPublisher * subscribers;
    final double rate = (double) totalMessages / took * 1000;
    final long bps = (long) (rate * messageSize * 8 * 2);
    
    if (options.verbose) {
      System.out.format("%,d msgs took %,d ms, %,.0f msg/s, %s\n", totalMessages, took, rate, Bandwidth.translate(bps));
    }
    verifyNoError(eh);
    
    afterBase();
    beforeBase();
  }
  
  public static void main(String[] args) {
    Testmark.enable();
    JUnitCore.runClasses(PubSubTest.class);
  }
}
