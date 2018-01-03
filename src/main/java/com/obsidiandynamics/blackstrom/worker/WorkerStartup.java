package com.obsidiandynamics.blackstrom.worker;

@FunctionalInterface
public interface WorkerStartup {
  void handle(WorkerThread thread);
}
