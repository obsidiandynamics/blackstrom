package com.obsidiandynamics.blackstrom.tracer;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.worker.*;

public final class LazyFiringStrategy extends FiringStrategy {
  private Action pending;
  
  public LazyFiringStrategy(AtomicReference<Action> tail) {
    super(tail);
  }

  @Override
  public void cycle(WorkerThread thread) throws InterruptedException {
    if (current != null) {
      if (current.isAnchor()) {
        // skip the anchor
      } else if (current.isComplete()) {
        pending = current;
      } else {
        if (pending != null) { 
          pending.fire();
          pending = null;
        }
        
        Thread.sleep(CYCLE_IDLE_INTERVAL_MILLIS);
        return;
      }
    } else {
      Thread.sleep(CYCLE_IDLE_INTERVAL_MILLIS);
    }
    
    current = head.next();
    if (current != null) {
      head = current;
    } else if (pending != null) { 
      pending.fire();
      pending = null;
    }
  }
}
