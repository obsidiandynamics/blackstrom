package com.obsidiandynamics.blackstrom.hazelcast.queue;

@FunctionalInterface
public interface PublishCallback {
  static PublishCallback nop() { return (offset, error) -> {}; }
  
  void onComplete(long offset, Throwable error);
}
