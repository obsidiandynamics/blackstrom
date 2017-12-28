package com.obsidiandynamics.blackstrom.worker;

@FunctionalInterface
public interface Worker {
  void cycle(WorkerThread thread) throws InterruptedException;
}
