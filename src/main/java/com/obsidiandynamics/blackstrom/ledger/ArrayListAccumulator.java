package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class ArrayListAccumulator implements Accumulator {
  private class Buffer {
    private volatile Buffer previous;
    private final long baseOffset;
    private final AppendOnlyArray items = new AppendOnlyArray(bufferSize);
    private volatile Buffer next;
    
    Buffer(Buffer previous, long baseOffset) {
      this.previous = previous;
      this.baseOffset = baseOffset;
    }
    
    int retrieve(long fromOffset, List<Message> sink) {
      final int length = items.size(); // volatile piggyback (inside the AppendOnlyArray)
      final int startIndex;
      if (fromOffset > baseOffset) {
        startIndex = (int) Math.min(fromOffset - baseOffset, length);
      } else {
        startIndex = 0;
      }
      for (int i = startIndex; i < length; i++) {
        sink.add(items.get(i));
      }
      return length - startIndex;
    }
  }

  private final int shard;
  private final int bufferSize;
  private final int retainBuffers;
  private volatile long nextOffset;
  private volatile Buffer latest;
  private Buffer earliest;
  
  private final Object lock = new Object();
  private int numBuffers = 1;
  
  ArrayListAccumulator(int shard, int bufferSize, int retainBuffers, long baseOffset) {
    this.shard = shard;
    this.bufferSize = bufferSize;
    this.retainBuffers = retainBuffers;
    earliest = latest = new Buffer(null, baseOffset);
    nextOffset = baseOffset;
  }

  @Override
  public void append(Message message) {
    synchronized (lock) {
      if (latest.items.size() == bufferSize) {
        createNextBuffer();
        if (numBuffers == retainBuffers) {
          removeEarliestBuffer();
        } else {
          numBuffers++;
        }
      }
      final ShardMessageId messageId = new ShardMessageId(shard, nextOffset);
      message.setMessageId(messageId);
      latest.items.add(message);
      nextOffset++;
    }
  }
  
  private void createNextBuffer() {
    final Buffer next = new Buffer(latest, latest.baseOffset + latest.items.size());
    latest.next = next;
    latest = next;
  }
  
  private void removeEarliestBuffer() {
    final Buffer second = earliest.next;
    second.previous = null;
    earliest = second;
  }

  @Override
  public long getNextOffset() {
    return nextOffset;
  }

  @Override
  public int retrieve(long fromOffset, List<Message> sink) {
    int totalRetrieved = 0;
    for (Buffer buffer = findBuffer(fromOffset);;) {
      final boolean bufferFull = buffer.next != null;
      totalRetrieved += buffer.retrieve(fromOffset, sink);
      if (bufferFull) {
        buffer = buffer.next;
      } else {
        break;
      }
    }
    return totalRetrieved;
  }
  
  private Buffer findBuffer(long offset) {
    Buffer buffer = latest;
    Buffer previous;
    while (buffer.baseOffset > offset && (previous = buffer.previous) != null) {
      buffer = previous;
    }
    return buffer;
  }
  
  public static Accumulator.Factory factory(int bufferSize) {
    return factory(bufferSize, Integer.MAX_VALUE);
  }
  
  public static Accumulator.Factory factory(int bufferSize, int retainBuffers) {
    return shard -> new ArrayListAccumulator(shard, bufferSize, retainBuffers, 0);
  }
}
