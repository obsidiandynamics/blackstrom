package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class ArrayListAccumulatorTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private final Timesert wait = Wait.SHORT;
  
  @Test
  public void testHalfOfSingleBuffer() {
    final long baseOffset = 0;
    final int bufferSize = 10;
    final int retainBuffers = 1;
    final Accumulator a = ArrayListAccumulator.factory(bufferSize, retainBuffers).create(0);
    LongList.generate(0, 5).toMessages().forEach(a::append);
    assertEquals(baseOffset + 5, a.getNextOffset());

    assertEquals(LongList.generate(0, 5), getItems(a, baseOffset));
    assertEquals(LongList.generate(0, 5).plus(baseOffset), getOffsets(a, baseOffset));
    
    assertEquals(LongList.generate(0, 5), getItems(a, baseOffset - 1));
    assertEquals(LongList.empty(), getItems(a, baseOffset + 5));
    assertEquals(LongList.empty(), getItems(a, baseOffset + 10));
    assertEquals(LongList.empty(), getItems(a, baseOffset + 11));
  }
  
  @Test
  public void testSingleBuffer() {
    final long baseOffset = Integer.MAX_VALUE;
    final int bufferSize = 10;
    final int retainBuffers = 1;
    final Accumulator a = new ArrayListAccumulator(0, bufferSize, retainBuffers, baseOffset);
    LongList.generate(0, 10).toMessages().forEach(a::append);
    assertEquals(baseOffset + 10, a.getNextOffset());

    assertEquals(LongList.generate(0, 10), getItems(a, baseOffset));
    assertEquals(LongList.generate(0, 10).plus(baseOffset), getOffsets(a, baseOffset));
    
    assertEquals(LongList.generate(0, 10), getItems(a, baseOffset - 1));
    assertEquals(LongList.generate(5, 10), getItems(a, baseOffset + 5));
    assertEquals(LongList.empty(), getItems(a, baseOffset + 10));
    assertEquals(LongList.empty(), getItems(a, baseOffset + 11));
  }
  
  @Test
  public void testMultipleBuffers() {
    final long baseOffset = Integer.MAX_VALUE;
    final int bufferSize = 10;
    final int retainBuffers = 3;
    final Accumulator a = new ArrayListAccumulator(0, bufferSize, retainBuffers, baseOffset);
    LongList.generate(0, 30).toMessages().forEach(a::append);
    assertEquals(baseOffset + 30, a.getNextOffset());

    assertEquals(LongList.generate(0, 30), getItems(a, baseOffset));
    assertEquals(LongList.generate(0, 30).plus(baseOffset), getOffsets(a, baseOffset));

    assertEquals(LongList.generate(0, 30), getItems(a, Long.MIN_VALUE));
    assertEquals(LongList.generate(0, 30), getItems(a, -1));
    assertEquals(LongList.generate(0, 30), getItems(a, 0));
    assertEquals(LongList.generate(0, 30), getItems(a, baseOffset - 1));
    assertEquals(LongList.generate(5, 30), getItems(a, baseOffset + 5));
    assertEquals(LongList.generate(10, 30), getItems(a, baseOffset + 10));
    assertEquals(LongList.generate(11, 30), getItems(a, baseOffset + 11));
    assertEquals(LongList.generate(11, 30).plus(baseOffset), getOffsets(a, baseOffset + 11));
    assertEquals(LongList.generate(19, 30), getItems(a, baseOffset + 19));
    assertEquals(LongList.generate(20, 30), getItems(a, baseOffset + 20));
    assertEquals(LongList.generate(21, 30), getItems(a, baseOffset + 21));
    assertEquals(LongList.generate(29, 30), getItems(a, baseOffset + 29));
    assertEquals(LongList.empty(), getItems(a, baseOffset + 30));
    assertEquals(LongList.empty(), getItems(a, baseOffset + 31));
    assertEquals(LongList.empty(), getItems(a, Long.MAX_VALUE));
  }
  
  @Test
  public void testMultipleBuffersWithReducedRetention() {
    final long baseOffset = Integer.MAX_VALUE;
    final int bufferSize = 10;
    final int retainBuffers = 2;
    final Accumulator a = new ArrayListAccumulator(0, bufferSize, retainBuffers, baseOffset);
    LongList.generate(0, 30).toMessages().forEach(a::append);
    assertEquals(baseOffset + 30, a.getNextOffset());

    assertEquals(LongList.generate(10, 30), getItems(a, baseOffset));
    assertEquals(LongList.generate(10, 30).plus(baseOffset), getOffsets(a, baseOffset));

    assertEquals(LongList.generate(10, 30), getItems(a, Long.MIN_VALUE));
    assertEquals(LongList.generate(10, 30), getItems(a, -1));
    assertEquals(LongList.generate(10, 30), getItems(a, 0));
    assertEquals(LongList.generate(10, 30), getItems(a, baseOffset - 1));
    assertEquals(LongList.generate(10, 30), getItems(a, baseOffset + 5));
    assertEquals(LongList.generate(10, 30), getItems(a, baseOffset + 10));
    assertEquals(LongList.generate(11, 30), getItems(a, baseOffset + 11));
    assertEquals(LongList.generate(11, 30).plus(baseOffset), getOffsets(a, baseOffset + 11));
    assertEquals(LongList.generate(19, 30), getItems(a, baseOffset + 19));
    assertEquals(LongList.generate(20, 30), getItems(a, baseOffset + 20));
    assertEquals(LongList.generate(21, 30), getItems(a, baseOffset + 21));
    assertEquals(LongList.generate(29, 30), getItems(a, baseOffset + 29));
    assertEquals(LongList.empty(), getItems(a, baseOffset + 30));
    assertEquals(LongList.empty(), getItems(a, baseOffset + 31));
    assertEquals(LongList.empty(), getItems(a, Long.MAX_VALUE));
  }
  
  private static class ProducerBallotId {
    final int producer;
    final long sequence;
    
    ProducerBallotId(int producer, long sequence) {
      this.producer = producer;
      this.sequence = sequence;
    }
    
    @Override
    public String toString() {
      return "[producer=" + producer + ", sequence=" + sequence + "]";
    }
  }
  
  private static class BallotReceiver implements MessageHandler {
    final List<Long>[] received;
    
    BallotReceiver(int numProducers) {
      received = Cast.from(new List[numProducers]);
      Arrays.setAll(received, i -> new CopyOnWriteArrayList<>());
    }

    @Override
    public String getGroupId() {
      return null;
    }

    @Override
    public void onMessage(MessageContext context, Message message) {
      final ProducerBallotId producerBallotId = (ProducerBallotId) message.getBallotId();
      received[producerBallotId.producer].add(producerBallotId.sequence);
    }
    
    Runnable receivedAll(LongList expected) {
      return () -> Arrays.stream(received).forEach(list -> assertEquals(expected, list));
    }
    
    Runnable receivedInOrder() {
      return () -> {
        for (List<Long> list : received) {
          final List<Long> copy = new ArrayList<>(list);
          Collections.sort(copy);
          assertEquals(copy, list);
        }
      };
    }
  }

  @Test
  public void testMultiProducerMultiConsumer() {
    final long baseOffset = Integer.MAX_VALUE;
    final int bufferSize = 10;
    final int numProducers = 5;
    final int numConsumers = 13;
    final int messagesPerProducer = 20;
    final int retainBuffers = numProducers * messagesPerProducer / bufferSize;
    final Accumulator a = new ArrayListAccumulator(0, bufferSize, retainBuffers, baseOffset);
    
    final List<BallotReceiver> receivers = IntStream.range(0, numConsumers).boxed()
        .map(c -> new BallotReceiver(numProducers)).collect(Collectors.toList());
    
    final List<AccumulatorConsumer> consumers = IntStream.range(0, numConsumers).boxed()
        .map(c -> new AccumulatorConsumer(a, receivers.get(c))).collect(Collectors.toList());
    
    boolean success = false;
    try {
      final CyclicBarrier barrier = new CyclicBarrier(numProducers + 1);
      final LongList expected = LongList.generate(0, messagesPerProducer);
      IntStream.range(0, numProducers).forEach(p -> {
        new Thread(() -> {
          expected.forEach(i -> a.append(new UnknownMessage(new ProducerBallotId(p, i), 0).withShard(0)));
          TestSupport.await(barrier);
        }).start();
      });
      TestSupport.await(barrier);
      
      final List<Message> sink = new ArrayList<>();
      a.retrieve(0, sink);
      assertEquals(numProducers * messagesPerProducer, sink.size());
      
      for (BallotReceiver receiver : receivers) {
        wait.until(receiver.receivedAll(expected));
      }
      success = true;
    } finally {
      if (! success) {
        final List<Message> sink = new ArrayList<>();
        a.retrieve(0, sink);
        System.out.println("accumulator:");
        sink.stream().map(m -> m.getBallotId()).forEach(id -> System.out.println("  " + id));
        
        for (BallotReceiver receiver : receivers) {
          System.out.println("receiver:");
          for (int i = 0; i < receiver.received.length; i++) {
            System.out.println("  received[" + i + "]=" + receiver.received[i]);
          }
        }
      }
      consumers.forEach(consumer -> consumer.dispose());
    }
  }

  @Test
  public void testMultiProducerMultiConsumerWithReducedRetention() {
    final long baseOffset = Integer.MAX_VALUE;
    final int bufferSize = 10;
    final int numProducers = 5;
    final int numConsumers = 13;
    final int messagesPerProducer = 20;
    final int retentionDivider = 2;
    final int retainBuffers = numProducers * messagesPerProducer / bufferSize / retentionDivider;
    final Accumulator a = new ArrayListAccumulator(0, bufferSize, retainBuffers, baseOffset);
    
    final List<BallotReceiver> receivers = IntStream.range(0, numConsumers).boxed()
        .map(c -> new BallotReceiver(numProducers)).collect(Collectors.toList());
    
    final List<AccumulatorConsumer> consumers = IntStream.range(0, numConsumers).boxed()
        .map(c -> new AccumulatorConsumer(a, receivers.get(c))).collect(Collectors.toList());
    
    boolean success = false;
    try {
      final CyclicBarrier barrier = new CyclicBarrier(numProducers + 1);
      final LongList expected = LongList.generate(0, messagesPerProducer);
      IntStream.range(0, numProducers).forEach(p -> {
        new Thread(() -> {
          expected.forEach(i -> a.append(new UnknownMessage(new ProducerBallotId(p, i), 0).withShard(0)));
          TestSupport.await(barrier);
        }).start();
      });
      TestSupport.await(barrier);
      
      for (BallotReceiver receiver : receivers) {
        wait.until(receiver.receivedInOrder());
      }
      success = true;
    } finally {
      if (! success) {
        final List<Message> sink = new ArrayList<>();
        a.retrieve(0, sink);
        System.out.println("accumulator:");
        sink.stream().map(m -> m.getBallotId()).forEach(id -> System.out.println("  " + id));
        
        for (BallotReceiver receiver : receivers) {
          System.out.println("receiver:");
          for (int i = 0; i < receiver.received.length; i++) {
            System.out.println("  received[" + i + "]=" + receiver.received[i]);
          }
        }
      }
      consumers.forEach(consumer -> consumer.dispose());
    }
  }
  
  private static List<Long> getItems(Accumulator a, long fromOffset) {
    return retrieve(a, fromOffset).stream()
        .map(m -> (Long) m.getBallotId())
        .collect(Collectors.toList());
  }
  
  private static List<Long> getOffsets(Accumulator a, long fromOffset) {
    return retrieve(a, fromOffset).stream()
        .map(m -> (ShardMessageId) m.getMessageId())
        .map(id -> id.getOffset())
        .collect(Collectors.toList());
  }
  
  private static List<Message> retrieve(Accumulator a, long fromOffset) {
    final List<Message> sink = new ArrayList<>();
    a.retrieve(fromOffset, sink);
    return sink;
  }
}
