package com.obsidiandynamics.blackstrom.tracer;

import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.worker.*;

public final class Tracer implements Joinable {
  private final WorkerThread executor;
  
  protected final AtomicReference<Action> tail = new AtomicReference<>(Action.anchor());
  
  /** Atomically assigns sequence numbers for thread naming. */
  private static final AtomicInteger nextThreadNo = new AtomicInteger();
  
  public Tracer(FiringStrategyFactory completionStrategyFactory) {
    this(completionStrategyFactory, Tracer.class.getSimpleName() + "-" + nextThreadNo.getAndIncrement());
  }
  
  public Tracer(FiringStrategyFactory firingStrategyFactory, String threadName) {
    executor = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withDaemon(true)
                     .withName(threadName))
        .onCycle(firingStrategyFactory.apply(tail))
        .build();
    executor.start();
  }
  
  public Action begin(Runnable task) {
    final Action action = new Action(task);
    action.appendTo(tail);
    return action;
  }
  
  /**
   *  Terminates the tracer, shutting down the worker thread and preventing further 
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
