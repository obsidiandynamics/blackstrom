package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.*;
import java.util.stream.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class ArrayListAccumulatorTest {
  @Test
  public void testHalfOfSingleBuffer() {
    final long baseOffset = 0;
    final int bufferSize = 10;
    final Accumulator a = ArrayListAccumulator.factory(bufferSize).create(0);
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
    final Accumulator a = new ArrayListAccumulator(0, bufferSize, baseOffset);
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
    final Accumulator a = new ArrayListAccumulator(0, bufferSize, baseOffset);
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
  
  private static class LongList extends ArrayList<Long> {
    private static final long serialVersionUID = 1L;
    
    private LongList(int size) {
      super(size);
    }
    
    LongList plus(long amount) {
      for (int i = 0; i < size(); i++) {
        set(i, get(i) + amount);
      }
      return this;
    }
    
    List<Message> toMessages() {
      return stream().map(i -> new UnknownMessage(i, 0)).collect(Collectors.toList());
    }
    
    static LongList generate(long firstInclusive, long lastExclusive) {
      final LongList list = new LongList((int) (lastExclusive - firstInclusive));
      for (long i = firstInclusive; i < lastExclusive; i++) {
        list.add(i);
      }
      return list;
    }
    
    static LongList empty() {
      return generate(0, 0);
    }
  }
}
