package com.obsidiandynamics.blackstrom.worker;

@FunctionalInterface
public interface WorkerExceptionHandler {
  void handle(WorkerThread thread, Throwable exception);
}
