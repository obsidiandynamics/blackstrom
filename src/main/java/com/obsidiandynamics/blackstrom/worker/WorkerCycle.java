package com.obsidiandynamics.blackstrom.worker;

@FunctionalInterface
public interface WorkerCycle {
  void cycle(WorkerThread thread) throws InterruptedException;
}
