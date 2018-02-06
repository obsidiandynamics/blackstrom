package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.nodequeue.*;
import com.obsidiandynamics.blackstrom.worker.*;

/**
 *  A high-performance, lock-free, unbounded MPMC (multi-producer, multi-consumer) queue
 *  implementation, adapted from Indigo's scheduler.<p>
 *  
 *  @see <a href="https://github.com/obsidiandynamics/indigo/blob/4b13815d1aefb0e5a5a45ad89444ced9f6584e20/src/main/java/com/obsidiandynamics/indigo/NodeQueueActivation.java">NodeQueueActivation</a>
 */
public final class MultiNodeQueueLedger implements Ledger {
  private static final int POLL_BACKOFF_MILLIS = 1;
  
  /** Tracks presence of group members. */
  private final Set<String> groups = new HashSet<>();
  
  private final List<WorkerThread> threads = new CopyOnWriteArrayList<>();
  
  private final MessageContext context = new DefaultMessageContext(this, null);

  private final NodeQueue<Message> queue = new NodeQueue<>();
  
  private class NodeWorker implements WorkerCycle {
    private final MessageHandler handler;
    private final QueueConsumer<Message> consumer;
    
    NodeWorker(MessageHandler handler, QueueConsumer<Message> consumer) {
      this.handler = handler;
      this.consumer = consumer;
    }
    
    @Override
    public void cycle(WorkerThread thread) throws InterruptedException {
      final Message m = consumer.poll();
      if (m != null) {
        handler.onMessage(context, m);
      } else {
        Thread.sleep(POLL_BACKOFF_MILLIS);
      }
    }
  }
  
  @Override
  public void attach(MessageHandler handler) {
    if (handler.getGroupId() != null && ! groups.add(handler.getGroupId())) return;
    
    final WorkerThread thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName(MultiNodeQueueLedger.class.getSimpleName() + "-" + handler.getGroupId())
                     .withDaemon(true))
        .onCycle(new NodeWorker(handler, queue.consumer()))
        .buildAndStart();
    threads.add(thread);
  }
  
  @Override
  public void append(Message message, AppendCallback callback) {
    queue.add(message);
    callback.onAppend(message.getMessageId(), null);
  }
  
  @Override
  public void confirm(Object handlerId, MessageId messageId) {}
  
  @Override
  public void dispose() {
    threads.forEach(t -> t.terminate());
    threads.forEach(t -> t.joinQuietly());
  }
}
