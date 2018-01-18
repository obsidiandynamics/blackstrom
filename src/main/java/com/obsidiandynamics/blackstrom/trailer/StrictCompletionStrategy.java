package com.obsidiandynamics.blackstrom.trailer;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.worker.*;

public final class StrictCompletionStrategy extends CompletionStrategy {
  public StrictCompletionStrategy(AtomicReference<Action> tail) {
    super(tail);
  }

  @Override
  public void cycle(WorkerThread thread) throws InterruptedException {
    if (current != null) {
      if (current.isAnchor()) {
        // skip the anchor
      } else if (current.isComplete()) {
        current.run();
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
