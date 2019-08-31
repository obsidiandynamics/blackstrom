package com.obsidiandynamics.blackstrom.ledger;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.nodequeue.*;
import com.obsidiandynamics.worker.*;

/**
 *  A high-performance, lock-free, unbounded MPSC (multi-producer, single-consumer) queue
 *  implementation, adapted from Indigo's scheduler.<p>
 *  
 *  @see <a href="https://github.com/obsidiandynamics/indigo/blob/4b13815d1aefb0e5a5a45ad89444ced9f6584e20/src/main/java/com/obsidiandynamics/indigo/NodeQueueActivation.java">NodeQueueActivation</a>
 */
public final class SingleNodeQueueLedger implements Ledger {
  private static final int POLL_BACKOFF_MILLIS = 1;
  
  public static final class Config {
    int maxYields = 100;
    
    public Config withMaxYields(int maxYields) {
      this.maxYields = maxYields;
      return this;
    }

    public void validate() {
      mustBeGreaterOrEqual(maxYields, 0, illegalArgument("Max yields must be greater or equal to 0"));
    }
  }
  
  /** Tracks presence of group members. */
  private final Set<String> groups = new CopyOnWriteArraySet<>();
  
  private static final class ContextualHandler {
    final MessageHandler handler;
    
    final MessageContext context;

    ContextualHandler(MessageHandler handler, MessageContext context) {
      this.handler = handler;
      this.context = context;
    }
  }
  
  private volatile ContextualHandler[] contextualHandlers = new ContextualHandler[0];
  
  /** Handler IDs that have been admitted to group-based message consumption. */
  private final Set<UUID> subscribedHandlerIds = new CopyOnWriteArraySet<>();
  
  private final WorkerThread thread;
  
  private final NodeQueue<Message> queue = new NodeQueue<>();
  
  private final QueueConsumer<Message> consumer = queue.consumer();
  
  private final int maxYields;
  
  private int yields;
  
  public SingleNodeQueueLedger() {
    this(new Config());
  }
  
  public SingleNodeQueueLedger(Config config) {
    mustExist(config, "Config cannot be null").validate();
    maxYields = config.maxYields;
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .daemon()
                     .withName(SingleNodeQueueLedger.class, Integer.toHexString(System.identityHashCode(this))))
        .onCycle(this::cycle)
        .buildAndStart();
  }
  
  private void cycle(WorkerThread thread) throws InterruptedException {
    final Message m = consumer.poll();
    if (m != null) {
      for (ContextualHandler contextualHandler : contextualHandlers) {
        contextualHandler.handler.onMessage(contextualHandler.context, m);
      }
    } else if (yields++ < maxYields) {
      Thread.yield();
    } else {
      // resetting yields here appears counterintuitive (it makes more sense to reset it on a hit than a miss),
      // however, this technique avoids writing to an instance field from a hotspot, markedly improving performance
      // at the expense of (1) prematurely sleeping on the next miss and (2) yielding after a sleep
      yields = 0;
      Thread.sleep(POLL_BACKOFF_MILLIS);
    }
  }
  
  @Override
  public Object attach(MessageHandler handler) {
    final UUID handlerId = handler.getGroupId() != null ? UUID.randomUUID() : null;
    
    if (handler.getGroupId() == null || groups.add(handler.getGroupId())) {
      final MessageContext context = new DefaultMessageContext(this, handlerId, NopRetention.getInstance());
      contextualHandlers = ArrayCopy.append(contextualHandlers, new ContextualHandler(handler, context));
      ifPresent(handlerId, subscribedHandlerIds::add);
    }
    
    return handlerId;
  }

  @Override
  public void append(Message message, AppendCallback callback) {
    queue.add(message);
    callback.onAppend(message.getMessageId(), null);
  }

  @Override
  public boolean isAssigned(Object handlerId, int shard) {
    return handlerId == null || subscribedHandlerIds.contains(handlerId);
  }

  @Override
  public void dispose() {
    thread.terminate().joinSilently();
  }
}
