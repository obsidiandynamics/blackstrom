package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;

/**
 *  A high-performance, lock-free, unbounded MPMC (multi-producer, multi-consumer) queue
 *  implementation, adapted from Indigo's scheduler.<p>
 *  
 *  @see <a href="https://github.com/obsidiandynamics/indigo/blob/4b13815d1aefb0e5a5a45ad89444ced9f6584e20/src/main/java/com/obsidiandynamics/indigo/NodeQueueActivation.java">NodeQueueActivation</a>
 */
public final class MultiNodeQueueLedger implements Ledger {
  private final List<WorkerThread> threads = new CopyOnWriteArrayList<>();
  
  private final MessageContext context = new DefaultMessageContext(this);
  
  private final AtomicReference<QueueNode> tail = new AtomicReference<>(QueueNode.anchor());
  
  private class NodeWorker implements WorkerCycle {
    private final MessageHandler handler;
    private AtomicReference<QueueNode> head;
    
    NodeWorker(MessageHandler handler, AtomicReference<QueueNode> head) {
      this.handler = handler;
      this.head = head;
    }
    
    @Override
    public void cycle(WorkerThread thread) throws InterruptedException {
      final QueueNode n = head.get();
      if (n != null) {
        handler.onMessage(context, n.m);
        head = n;
      } else {
        Thread.sleep(1);
      }
    }
  }
  
  @Override
  public void attach(MessageHandler handler) {
    final WorkerThread thread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName("NodeWorker-" + Long.toHexString(System.identityHashCode(handler)))
                     .withDaemon(true))
        .onCycle(new NodeWorker(handler, tail.get()))
        .build();
    threads.add(thread);
    thread.start();
  }
  
  @Override
  public void append(Message message) throws InterruptedException {
    new QueueNode(message).appendTo(tail);
  }
  
  @Override
  public void dispose() {
    threads.forEach(t -> t.terminate());
    threads.forEach(t -> t.joinQuietly());
  }
}
