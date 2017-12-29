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
public final class NodeQueueLedger implements Ledger {
  private final List<WorkerThread> threads = new CopyOnWriteArrayList<>();
  
  private final MessageContext context = new DefaultVotingContext(this);
  
  private static final class Node extends AtomicReference<Node> {
    private static final long serialVersionUID = 1L;

    private final Message m;

    Node(Message m) { this.m = m; }
    
    static Node anchor() {
      return new Node(null);
    }
  }

  private final AtomicReference<Node> tail = new AtomicReference<>(Node.anchor());
  
  private class NodeWorker implements Worker {
    private final MessageHandler handler;
    private AtomicReference<Node> head;
    
    NodeWorker(MessageHandler handler, AtomicReference<Node> head) {
      this.handler = handler;
      this.head = head;
    }
    
    @Override
    public void cycle(WorkerThread thread) throws InterruptedException {
      final Node n = head.get();
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
        .withWorker(new NodeWorker(handler, tail.get()))
        .build();
    threads.add(thread);
    thread.start();
  }
  
  @Override
  public void append(Message message) throws InterruptedException {
    final Node t = new Node(message);
    final Node t1 = tail.getAndSet(t);
    t1.lazySet(t);
  }
  
  @Override
  public void dispose() {
    threads.forEach(t -> t.terminate());
    threads.forEach(t -> t.joinQuietly());
  }
}
