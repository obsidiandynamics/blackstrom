package com.obsidiandynamics.blackstrom.worker;

public interface Worker {
  void cycle(WorkerThread thread) throws InterruptedException;
}
