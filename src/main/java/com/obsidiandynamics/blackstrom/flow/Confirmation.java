package com.obsidiandynamics.blackstrom.flow;

import java.util.concurrent.atomic.*;

public final class Confirmation {
  private final AtomicReference<Confirmation> next = new AtomicReference<>();
  
  private final Runnable task;
  
  private volatile boolean confirmed;
  
  Confirmation(Runnable task) {
    this.task = task;
  }
  
  public void confirm() {
    confirmed = true;
  }
  
  boolean isAnchor() {
    return task == null;
  }
  
  boolean isConfirmed() {
    return confirmed;
  }
  
  void appendTo(AtomicReference<Confirmation> tail) {
    final Confirmation t1 = tail.getAndSet(this);
    t1.next.lazySet(this);
  }
  
  void fire() {
    task.run();
  }
  
  Confirmation next() {
    return next.get();
  }
  
  @Override
  public String toString() {
    return Confirmation.class.getSimpleName() + " [task=" + task + ", confirmed=" + confirmed + ", next=" + next + "]";
  }

  static Confirmation anchor() {
    return new Confirmation(null);
  }
}
