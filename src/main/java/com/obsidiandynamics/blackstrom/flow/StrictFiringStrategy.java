package com.obsidiandynamics.blackstrom.flow;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.worker.*;

public final class StrictFiringStrategy extends FiringStrategy {
  public StrictFiringStrategy(AtomicReference<Confirmation> tail) {
    super(tail);
  }

  @Override
  public void cycle(WorkerThread thread) throws InterruptedException {
    if (current != null) {
      if (current.isAnchor()) {
        // skip the anchor
      } else if (current.isConfirmed()) {
        current.fire();
      } else {
        Thread.sleep(CYCLE_IDLE_INTERVAL_MILLIS);
        return;
      }
    } else {
      Thread.sleep(CYCLE_IDLE_INTERVAL_MILLIS);
    }
    
    current = head.next();
    if (current != null) {
      head = current;
    }
  }
}
