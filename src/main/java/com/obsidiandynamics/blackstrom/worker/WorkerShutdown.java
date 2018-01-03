package com.obsidiandynamics.blackstrom.worker;

@FunctionalInterface
public interface WorkerShutdown {
  void handle(WorkerThread thread, Throwable exception);
}
