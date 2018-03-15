package com.obsidiandynamics.blackstrom.hazelcast.queue;

@FunctionalInterface
public interface PublishCallback {
  void onComplete(long offset, Exception exception);
}
