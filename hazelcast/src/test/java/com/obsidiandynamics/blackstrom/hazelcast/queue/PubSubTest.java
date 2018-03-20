package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.junit.Assert.*;

import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import org.junit.*;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.hazelcast.*;
import com.obsidiandynamics.blackstrom.hazelcast.elect.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.Receiver.*;
import com.obsidiandynamics.blackstrom.util.*;

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
    testPubSub(3, 5, 2, null);
  }
  
  @Test
  public void testPubSubGroupAware() {
    testPubSub(3, 5, 2, randomGroup());
  }
  
  private void testPubSub(int numReceivers, int numMessages, int pooledInstances, String group) {
    final String stream = "s";
    
    // common configuration
    final StreamConfig streamConfig = new StreamConfig()
        .withName(stream);
    
    // create the pooled instances (shared by both publishers and subscribers)
    final InstancePool instances = new InstancePool(pooledInstances, this::newInstance);

    // create subscribers with receivers
    final ErrorHandler eh = mockErrorHandler();
    final SubscriberConfig subConfig = new SubscriberConfig()
        .withGroup(group)
        .withErrorHandler(eh)
        .withElectionConfig(new ElectionConfig().withScavengeInterval(1))
        .withStreamConfig(streamConfig);
        
    final List<TestHandler> handlers = new ArrayList<>(numReceivers);
    for (int i = 0; i < numReceivers; i++) {
      final HazelcastInstance instance = instances.get();
      final Subscriber s = configureSubscriber(instance, subConfig);
      createReceiver(s, register(new TestHandler(), handlers), 10);
    }
    
    // create a publisher and publish the messages
    final PublisherConfig pubConfig = new PublisherConfig()
        .withStreamConfig(streamConfig);
    final HazelcastInstance instance = instances.get();
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
        System.out.format("numReceivers=%d, numMessages=%d, pooledInstances=%d, group=%s\n",
                          numReceivers, numMessages, pooledInstances, group);
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
    testOneWay(2, 4, 10_000 * SCALE);
  }
  
  private void testOneWay(int producers, int consumers, int messagesPerProducer) {
    
  }
}
