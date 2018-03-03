package com.obsidiandynamics.blackstrom.flow;

import java.util.concurrent.atomic.*;
import java.util.function.*;

import com.obsidiandynamics.blackstrom.worker.*;

public abstract class FiringStrategy implements WorkerCycle {
  protected static final int CYCLE_IDLE_INTERVAL_MILLIS = 1;
  
  protected final AtomicReference<FlowConfirmation> tail;
  
  protected FlowConfirmation head;
  
  protected FlowConfirmation current;
  
  protected FiringStrategy(AtomicReference<FlowConfirmation> tail) {
    this.tail = tail;
    head = tail.get();
    current = head;
  }
  
  @FunctionalInterface
  public interface Factory extends Function<AtomicReference<FlowConfirmation>, FiringStrategy> {}
}
