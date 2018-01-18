package com.obsidiandynamics.blackstrom.trailer;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.worker.*;

public abstract class CompletionStrategy implements WorkerCycle {
  protected static final int CYCLE_IDLE_INTERVAL_MILLIS = 1;
  
  protected final AtomicReference<Action> tail;
  
  protected Action head;
  
  protected Action current;
  
  protected CompletionStrategy(AtomicReference<Action> tail) {
    this.tail = tail;
    head = tail.get();
    current = head;
  }
}
