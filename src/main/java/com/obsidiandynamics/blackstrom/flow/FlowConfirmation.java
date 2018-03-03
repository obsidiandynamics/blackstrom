package com.obsidiandynamics.blackstrom.flow;

import java.util.concurrent.atomic.*;

public final class FlowConfirmation implements Confirmation {
  private final AtomicReference<FlowConfirmation> next = new AtomicReference<>();
  
  private final Runnable task;
  
  private volatile boolean confirmed;
  
  FlowConfirmation(Runnable task) {
    this.task = task;
  }
  
  @Override
  public void confirm() {
    confirmed = true;
  }
  
  boolean isAnchor() {
    return task == null;
  }
  
  boolean isConfirmed() {
    return confirmed;
  }
  
  void appendTo(AtomicReference<FlowConfirmation> tail) {
    final FlowConfirmation t1 = tail.getAndSet(this);
    t1.next.lazySet(this);
  }
  
  void fire() {
    task.run();
  }
  
  FlowConfirmation next() {
    return next.get();
  }
  
  @Override
  public String toString() {
    return FlowConfirmation.class.getSimpleName() + " [task=" + task + ", confirmed=" + confirmed + ", next=" + next + "]";
  }

  static FlowConfirmation anchor() {
    return new FlowConfirmation(null);
  }
}
