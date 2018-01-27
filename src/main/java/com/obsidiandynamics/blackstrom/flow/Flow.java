package com.obsidiandynamics.blackstrom.flow;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.worker.*;

public final class Flow implements Joinable {
  private final WorkerThread executor;
  
  protected final AtomicReference<Confirmation> tail = new AtomicReference<>(Confirmation.anchor());
  
  /** Atomically assigns sequence numbers for thread naming. */
  private static final AtomicInteger nextThreadNo = new AtomicInteger();
  
  public Flow(FiringStrategy.Factory completionStrategyFactory) {
    this(completionStrategyFactory, Flow.class.getSimpleName() + "-" + nextThreadNo.getAndIncrement());
  }
  
  public Flow(FiringStrategy.Factory firingStrategyFactory, String threadName) {
    executor = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withDaemon(true)
                     .withName(threadName))
        .onCycle(firingStrategyFactory.apply(tail))
        .buildAndStart();
  }
  
  public Confirmation begin(Runnable task) {
    final Confirmation confirmation = new Confirmation(task);
    confirmation.appendTo(tail);
    return confirmation;
  }
  
  /**
   *  Terminates the flow, shutting down the worker thread and preventing further 
   *  task executions.
   *  
   *  @return A {@link Joinable} for the caller to wait on.
   */
  public Joinable terminate() {
    executor.terminate();
    return this;
  }
  
  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return executor.join(timeoutMillis);
  }
}
