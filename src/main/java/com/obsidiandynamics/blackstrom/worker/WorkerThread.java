package com.obsidiandynamics.blackstrom.worker;

public final class WorkerThread implements Joinable {
  private final Thread driver;
  
  private final WorkerCycle worker;
  
  private final WorkerStartup onStartup;
  
  private final WorkerShutdown onShutdown;
  
  private volatile WorkerState state = WorkerState.CONCEIVED;
  
  WorkerThread(WorkerOptions options, WorkerCycle worker, WorkerStartup onStartup, WorkerShutdown onShutdown) {
    this.worker = worker;
    this.onStartup = onStartup;
    this.onShutdown = onShutdown;
    driver = new Thread(this::run);
    
    if (options.getName() != null) {
      driver.setName(options.getName());
    }
    
    driver.setDaemon(options.isDaemon());
    driver.setPriority(options.getPriority());
  }
  
  Thread getDriver() {
    return driver;
  }
  
  /**
   *  Starts the worker thread.
   */
  public final void start() {
    if (state == WorkerState.CONCEIVED) {
      state = WorkerState.RUNNING;
      driver.start();
    } else {
      throw new IllegalStateException("Cannot start worker in state " + state);
    }
  }
  
  /**
   *  Terminates the worker thread.
   *  
   *  @return A {@link Joinable} for the caller to wait on.
   */
  public final Joinable terminate() {
    if (state == WorkerState.RUNNING) {
      state = WorkerState.TERMINATING;
      driver.interrupt();
    }
    return this;
  }
  
  private void run() {
    onStartup.handle(this);
    Throwable exception = null;
    try {
      while (state == WorkerState.RUNNING) {
        try {
          cycle();
        } catch (Throwable e) {
          state = WorkerState.TERMINATING;
          exception = e;
        }
      }
    } finally {
      onShutdown.handle(this, exception);
      state = WorkerState.TERMINATED;
    }
  }
  
  /**
   *  Obtains the current state of the worker thread.
   *  
   *  @return The thread's state.
   */
  public final WorkerState getState() {
    return state;
  }
  
  private void cycle() throws InterruptedException {
    worker.cycle(this);
  }
  
  @Override
  public final boolean join(long timeoutMillis) throws InterruptedException {
    driver.join(timeoutMillis);
    return ! driver.isAlive();
  }
  
  public final String getName() {
    return driver.getName();
  }
  
  public final boolean isDaemon() {
    return driver.isDaemon();
  }
  
  public final int getPriority() {
    return driver.getPriority();
  }
  
  @Override
  public final int hashCode() {
    return driver.hashCode();
  }

  @Override
  public final boolean equals(Object obj) {
    return obj instanceof WorkerThread ? driver.equals(((WorkerThread) obj).driver) : false;
  }

  @Override
  public final String toString() {
    return "WorkerThread [thread=" + driver + ", state=" + state + "]";
  }
  
  public static WorkerThreadBuilder builder() {
    return new WorkerThreadBuilder();
  }
}
