package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.model.*;

final class ArrayListAccumulator implements Accumulator {
  private static class Buffer {
    private final Buffer previous;
    private volatile long baseOffset;
    private final List<Message> list = new CopyOnWriteArrayList<>();
    private volatile Buffer next;
    
    Buffer(Buffer previous) {
      this.previous = previous;
    }
    
    void populate(List<Message> messages) {
      
    }
  }
  
  private final int shard;
  private volatile long nextOffset;
  
  private volatile Buffer earliest = new Buffer(null);
  private volatile Buffer latest = earliest;
  
  private final Object lock = new Object();
  
  ArrayListAccumulator(int shard) {
    this.shard = shard;
  }

  @Override
  public void append(Message message) {
    synchronized (lock) {
      final BalancedMessageId messageId = new BalancedMessageId(shard, nextOffset);
      message.withMessageId(messageId);
      latest.list.add(message);
      nextOffset++;
    }
  }

  @Override
  public long getNextOffset() {
    return nextOffset;
  }

  @Override
  public List<Message> retrieve(long fromOffset) {
    Buffer buffer = findBuffer(fromOffset);
    return null;
  }
  
  private Buffer findBuffer(long offset) {
    Buffer buffer = latest;
    while (buffer.baseOffset > offset) {
      buffer = buffer.previous;
    }
    return buffer;
  }
}
