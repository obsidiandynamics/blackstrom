package com.obsidiandynamics.blackstrom.worker;

import java.util.concurrent.atomic.*;

public final class WorkerThread implements Joinable {
  private final Thread driver;
  
  private final WorkerCycle worker;
  
  private final WorkerStartup onStartup;
  
  private final WorkerShutdown onShutdown;
  
  private volatile WorkerState state = WorkerState.CONCEIVED;
  
  /** Indicates that the shutdown handler is about to be run. */
  private final AtomicBoolean shutdown = new AtomicBoolean();
  
  /** Indicates that the driver has been interrupted by {@link #shutdown()}. */
  private volatile boolean interrupted;
  
  /** Guards the changing of the thread state. */
  private final Object stateLock = new Object();
  
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
    synchronized (stateLock) {
      if (state == WorkerState.CONCEIVED) {
        state = WorkerState.RUNNING;
        driver.start();
      } else {
        throw new IllegalStateException("Cannot start worker in state " + state);
      }
    }
  }
  
  /**
   *  Terminates the worker thread.
   *  
   *  @return A {@link Joinable} for the caller to wait on.
   */
  public final Joinable terminate() {
    synchronized (stateLock) {
      if (state == WorkerState.CONCEIVED) {
        state = WorkerState.TERMINATED;
      } else if (state == WorkerState.RUNNING) {
        state = WorkerState.TERMINATING;
      }
    }
    
    // only interrupt the driver if it hasn't finished cycling, and at most once
    if (shutdown.compareAndSet(false, true)) {
      driver.interrupt();
      interrupted = true;
    }
    
    return this;
  }
  
  private void run() {
    Throwable exception = null;
    try {
      onStartup.handle(this);
      while (state == WorkerState.RUNNING) {
        cycle();
      }
    } catch (Throwable e) {
      synchronized (stateLock) {
        state = WorkerState.TERMINATING;
      }
      exception = e;
    } finally {
      if (shutdown.compareAndSet(false, true)) {
        // indicate that we've finished cycling - this way we won't get interrupted and 
        // can call the shutdown hook safely
        onShutdown.handle(this, exception);
      } else {
        // we may get interrupted - wait before continuing with the shutdown hook
        while (! interrupted) Thread.yield();
        Thread.interrupted(); // clear the interrupt before invoking the shutdown hook
        onShutdown.handle(this, exception);
      }
      
      synchronized (stateLock) {
        state = WorkerState.TERMINATED;
      }
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
