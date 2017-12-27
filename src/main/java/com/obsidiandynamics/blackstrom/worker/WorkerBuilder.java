package com.obsidiandynamics.blackstrom.worker;

public final class WorkerBuilder {
  private WorkerOptions options;
  
  private Worker worker;
  
  WorkerBuilder() {}

  public WorkerBuilder withOptions(WorkerOptions options) {
    this.options = options;
    return this;
  }

  public WorkerBuilder withWorker(Worker worker) {
    this.worker = worker;
    return this;
  }
  
  public WorkerThread build() {
    return new WorkerThread(options, worker);
  }
}
