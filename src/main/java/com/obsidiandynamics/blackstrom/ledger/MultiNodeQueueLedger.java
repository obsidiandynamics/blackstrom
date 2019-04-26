package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.nodequeue.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;

/**
 *  A high-performance, lock-free, unbounded MPMC (multi-producer, multi-consumer) queue
 *  implementation, adapted from Indigo's scheduler.<p>
 *  
 *  @see <a href="https://github.com/obsidiandynamics/indigo/blob/4b13815d1aefb0e5a5a45ad89444ced9f6584e20/src/main/java/com/obsidiandynamics/indigo/NodeQueueActivation.java">NodeQueueActivation</a>
 */
public final class MultiNodeQueueLedger implements Ledger {
  private static final int POLL_BACKOFF_MILLIS = 1;
  
  public static final class Config {
    int maxYields = 100;
    
    int debugMessageCounts = 0;
    
    LogLine logLine = System.out::println;
    
    public Config withMaxYields(int maxYields) {
      this.maxYields = maxYields;
      return this;
    }
    
    public Config withDebugMessageCounts(int debugMessageCounts) {
      this.debugMessageCounts = debugMessageCounts;
      return this;
    }
    
    public Config withLogLine(LogLine logLine) {
      this.logLine = logLine;
      return this;
    }
  }
  
  /** Tracks presence of group members. */
  private final Set<String> groups = new CopyOnWriteArraySet<>();
  
  private final List<WorkerThread> threads = new CopyOnWriteArrayList<>();
  
  private final NodeQueue<Message> queue = new NodeQueue<>();
  
  private final int debugMessageCounts;
  
  private final int maxYields;
  
  private final LogLine logLine;
  
  /** Handler IDs that have been admitted to group-based message consumption. */
  private final Set<UUID> subscribedHandlerIds = new CopyOnWriteArraySet<>();
  
  private class NodeWorker implements WorkerCycle {
    private final MessageHandler handler;
    private final QueueConsumer<Message> consumer;
    private final String groupId;
    private final MessageContext context;
    private int yields;
    
    NodeWorker(MessageHandler handler, String groupId, QueueConsumer<Message> consumer) {
      this.handler = handler;
      this.groupId = groupId;
      this.consumer = consumer;
      
      final UUID handlerId;
      if (groupId != null) {
        handlerId = UUID.randomUUID();
        subscribedHandlerIds.add(handlerId);
      } else {
        handlerId = null;
      }
      context = new DefaultMessageContext(MultiNodeQueueLedger.this, handlerId, NopRetention.getInstance());
    }
    
    private final AtomicLong consumed = new AtomicLong();
    
    @Override
    public void cycle(WorkerThread thread) throws InterruptedException {
      final Message m = consumer.poll();
      if (m != null) {
        if (debugMessageCounts != 0) {
          final long consumed = this.consumed.getAndIncrement();
          if (consumed % debugMessageCounts == 0) {
            logLine.accept(String.format("groupId=%s, consumed=%,d", groupId, consumed));
          }
        }
        
        handler.onMessage(context, m);
      } else if (yields++ < maxYields) {
        Thread.yield();
      } else {
        // resetting yields here appears counterintuitive (it makes more sense to reset it on a hit than a miss);
        // however, this technique avoids writing to an instance field from a hotspot, markedly improving performance
        // at the expense of (1) prematurely sleeping on the next miss and (2) yielding after a sleep
        yields = 0;
        Thread.sleep(POLL_BACKOFF_MILLIS);
      }
    }
  }
  
  public MultiNodeQueueLedger() {
    this(new Config());
  }
  
  public MultiNodeQueueLedger(Config config) {
    maxYields = config.maxYields;
    debugMessageCounts = config.debugMessageCounts;
    logLine = config.logLine;
  }
  
  @Override
  public void attach(MessageHandler handler) {
    if (handler.getGroupId() != null && ! groups.add(handler.getGroupId())) return;
    
    final NodeWorker nodeWorker = new NodeWorker(handler, handler.getGroupId(), queue.consumer());
    final WorkerThread thread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(MultiNodeQueueLedger.class, handler.getGroupId()))
        .onCycle(nodeWorker)
        .buildAndStart();
    threads.add(thread);
  }
  
  private final AtomicLong appends = new AtomicLong();
  
  @Override
  public void append(Message message, AppendCallback callback) {
    if (debugMessageCounts != 0) {
      final long appends = this.appends.getAndIncrement();
      if (appends % debugMessageCounts == 0) {
        logLine.accept(String.format("appends=%,d", appends));
      }
    }
    
    queue.add(message);
    callback.onAppend(message.getMessageId(), null);
  }

  @Override
  public boolean isAssigned(Object handlerId, int shard) {
    return handlerId == null || subscribedHandlerIds.contains(handlerId);
  }
  
  @Override
  public void dispose() {
    Terminator.of(threads).terminate().joinSilently();
  }
}
