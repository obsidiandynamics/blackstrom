package com.obsidiandynamics.blackstrom.trailer;

import java.util.concurrent.atomic.*;

public final class Action {
  private final AtomicReference<Action> next = new AtomicReference<>();
  
  private final Runnable task;
  
  private volatile boolean complete;
  
  Action(Runnable task) {
    this.task = task;
  }
  
  public void complete() {
    complete = true;
  }
  
  boolean isAnchor() {
    return task == null;
  }
  
  boolean isComplete() {
    return complete;
  }
  
  void appendTo(AtomicReference<Action> tail) {
    final Action t1 = tail.getAndSet(this);
    t1.next.lazySet(this);
  }
  
  void run() {
    task.run();
  }
  
  Action next() {
    return next.get();
  }
  
  @Override
  public String toString() {
    return Action.class.getSimpleName() + " [task=" + task + ", complete=" + complete + ", next=" + next + "]";
  }

  static Action anchor() {
    return new Action(null);
  }
}
