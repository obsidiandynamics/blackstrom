package com.obsidiandynamics.blackstrom.flow;

import java.util.concurrent.atomic.*;
import java.util.function.*;

import com.obsidiandynamics.blackstrom.worker.*;

public abstract class FiringStrategy implements WorkerCycle {
  protected static final int CYCLE_IDLE_INTERVAL_MILLIS = 1;
  
  protected final AtomicReference<Confirmation> tail;
  
  protected Confirmation head;
  
  protected Confirmation current;
  
  protected FiringStrategy(AtomicReference<Confirmation> tail) {
    this.tail = tail;
    head = tail.get();
    current = head;
  }
  
  @FunctionalInterface
  public interface Factory extends Function<AtomicReference<Confirmation>, FiringStrategy> {}
}
