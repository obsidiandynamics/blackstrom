package com.obsidiandynamics.blackstrom.nodequeue;

import java.util.concurrent.atomic.*;

public final class QueueConsumer<T> {
  private AtomicReference<LinkedNode<T>> head;
  
  QueueConsumer(AtomicReference<LinkedNode<T>> head) {
    this.head = head;
  }
  
  public T poll() {
    final LinkedNode<T> n = head.get();
    if (n != null) {
      head = n;
      return n.item;
    } else {
      return null;
    }
  }
}