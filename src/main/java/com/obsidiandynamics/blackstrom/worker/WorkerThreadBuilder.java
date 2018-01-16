package com.obsidiandynamics.blackstrom.worker;

import java.io.*;
import java.util.function.*;

public final class WorkerThreadBuilder {
  private WorkerOptions options = new WorkerOptions();
  
  private WorkerCycle onCycle;
  
  private WorkerStartup onStartup = t -> {};
  
  private WorkerShutdown onShutdown = DEFAULT_UNCAUGHT_EXCEPTION_HANDLER;
  
  public static final WorkerShutdown DEFAULT_UNCAUGHT_EXCEPTION_HANDLER = createPrintStreamUncaughtExceptionHandler(System.err);
  
  public static WorkerShutdown createPrintStreamUncaughtExceptionHandler(PrintStream printStream) {
    return createUncaughtExceptionHandler(x -> x.printStackTrace(printStream));
  }
  
  public static WorkerShutdown createUncaughtExceptionHandler(Consumer<Throwable> exceptionHandler) {
    return (t, e) -> {
      if (e != null && ! (e instanceof InterruptedException)) {
        exceptionHandler.accept(e);
      }
    };
  }
  
  WorkerThreadBuilder() {}

  public WorkerThreadBuilder withOptions(WorkerOptions options) {
    this.options = options;
    return this;
  }

  public WorkerThreadBuilder onCycle(WorkerCycle onCycle) {
    this.onCycle = onCycle;
    return this;
  }
  
  public WorkerThreadBuilder onStartup(WorkerStartup onStartup) {
    this.onStartup = onStartup;
    return this;
  }
  
  public WorkerThreadBuilder onShutdown(WorkerShutdown onShutdown) {
    this.onShutdown = onShutdown;
    return this;
  }
  
  public WorkerThread build() {
    if (onCycle == null) {
      throw new IllegalStateException("onCycle behaviour not set");
    }
    
    return new WorkerThread(options, onCycle, onStartup, onShutdown);
  }
}
