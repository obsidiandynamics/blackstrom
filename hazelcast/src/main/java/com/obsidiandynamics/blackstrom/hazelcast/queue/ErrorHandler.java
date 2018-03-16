package com.obsidiandynamics.blackstrom.hazelcast.queue;

@FunctionalInterface
public interface ErrorHandler {
  void onError(String summary, Throwable error);
}
