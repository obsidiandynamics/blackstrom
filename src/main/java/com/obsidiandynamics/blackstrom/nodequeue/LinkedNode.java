package com.obsidiandynamics.blackstrom.nodequeue;

import java.util.concurrent.atomic.*;

public final class LinkedNode<T> extends AtomicReference<LinkedNode<T>> {
  private static final long serialVersionUID = 1L;

  public final T item;

  public LinkedNode(T item) { this.item = item; }
  
  public static <T> LinkedNode<T> anchor() {
    return new LinkedNode<>(null);
  }
  
  public void appendTo(AtomicReference<LinkedNode<T>> tail) {
    final LinkedNode<T> t1 = tail.getAndSet(this);
    t1.lazySet(this);
  }
}