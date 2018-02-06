package com.obsidiandynamics.blackstrom.nodequeue;

import java.util.concurrent.atomic.*;

/**
 *  A generic, high-performance, lock-free, unbounded MPMC (multi-producer, multi-consumer) queue
 *  implementation, adapted from Indigo's scheduler.<p>
 *  
 *  @see <a href="https://github.com/obsidiandynamics/indigo/blob/4b13815d1aefb0e5a5a45ad89444ced9f6584e20/src/main/java/com/obsidiandynamics/indigo/NodeQueueActivation.java">NodeQueueActivation</a>
 */
public final class NodeQueue<T> {
  private final AtomicReference<LinkedNode<T>> tail = new AtomicReference<>(LinkedNode.anchor());
  
  public void add(T item) {
    new LinkedNode<>(item).appendTo(tail);
  }
  
  public QueueConsumer<T> consumer() {
    return new QueueConsumer<>(tail.get());
  }
}
