package com.obsidiandynamics.blackstrom.worker;

public final class WorkerThreadBuilder {
  private WorkerOptions options = new WorkerOptions();
  
  private WorkerCycle onCycle;
  
  private WorkerStartup onStartup = t -> {};
  
  private WorkerShutdown onShutdown = (t, e) -> {};
  
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
