package com.obsidiandynamics.blackstrom.nodequeue;

import static org.junit.Assert.*;

import java.util.*;
import java.util.stream.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.nodequeue.NodeQueue.*;

public final class NodeQueueTest {
  @Test
  public void testLateConsumer() {
    final int messages = 100;
    final NodeQueue<Long> q = new NodeQueue<>();
    LongStream.range(0, messages).forEach(q::add);
    final List<Long> consumed = consume(q.consumer());
    assertEquals(0, consumed.size());
  }

  @Test
  public void testEarlyConsumer() {
    final int messages = 100;
    final NodeQueue<Long> q = new NodeQueue<>();
    final Consumer<Long> c = q.consumer();
    LongStream.range(0, messages).forEach(q::add);
    final List<Long> consumed = consume(c);
    assertEquals(messages, consumed.size());
  }
  
  private static List<Long> consume(Consumer<Long> consumer) {
    final List<Long> items = new ArrayList<>();
    for (Long item; (item = consumer.poll()) != null; items.add(item));
    return items;
  }
}
