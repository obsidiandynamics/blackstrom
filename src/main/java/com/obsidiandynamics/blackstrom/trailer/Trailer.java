package com.obsidiandynamics.blackstrom.trailer;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.worker.*;

public final class Trailer implements Joinable {
  private static final int CYCLE_IDLE_INTERVAL_MILLIS = 10;
  
  private final WorkerThread executor;
  
  private final AtomicReference<Action> tail = new AtomicReference<>(Action.anchor());
  
  private Action head = tail.get();
  
  private Action current = head;
  
  /** Atomically assigns sequence numbers for thread naming. */
  private static final AtomicInteger nextThreadNo = new AtomicInteger();
  
  public Trailer() {
    this(Trailer.class.getSimpleName() + "-" + nextThreadNo.getAndIncrement());
  }
  
  public Trailer(String threadName) {
    executor = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withDaemon(true)
                     .withName(threadName))
        .onCycle(this::cycle)
        .build();
    executor.start();
  }
  
  private void cycle(WorkerThread t) throws InterruptedException {
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
  
  public Action begin(Runnable task) {
    final Action action = new Action(task);
    action.appendTo(tail);
    return action;
  }
  
  /**
   *  Terminates the trailer, shutting down the worker thread and preventing further 
   *  task executions.
   *  
   *  @return A {@link Joinable} for the caller to wait on.
   */
  public Joinable terminate() {
    return executor.terminate();
  }
  
  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return executor.join(timeoutMillis);
  }
}
