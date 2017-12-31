package com.obsidiandynamics.blackstrom.worker;

public final class WorkerThread {
  private final Thread driver;
  
  private final Worker worker;
  
  private volatile WorkerState state = WorkerState.CONCEIVED;
  
  WorkerThread(WorkerOptions options, Worker worker) {
    this.worker = worker;
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
   */
  public final void terminate() {
    if (state == WorkerState.RUNNING) {
      state = WorkerState.TERMINATING;
      driver.interrupt();
    }
  }
  
  private void run() {
    while (state == WorkerState.RUNNING) {
      try {
        cycle();
      } catch (InterruptedException e) {
        terminate();
      }
    }
    state = WorkerState.TERMINATED;
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
  
  /**
   *  Waits until the worker thread terminates.
   *  
   *  @throws InterruptedException If the thread is interrupted.
   */
  public final void join() throws InterruptedException {
    driver.join();
  }
  
  /**
   *  Waits until the worker thread terminates.<p>
   *  
   *  This method suppresses an {@link InterruptedException} and will re-assert the interrupt 
   *  prior to returning.
   */
  public final void joinQuietly() {
    try {
      join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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
  
  public static WorkerBuilder builder() {
    return new WorkerBuilder();
  }
}
