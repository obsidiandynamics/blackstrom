package com.obsidiandynamics.blackstrom.hazelcast.queue;

@FunctionalInterface
public interface ErrorHandler {
  static ErrorHandler nop() { return (__summary, __error) -> {}; }
  
  void onError(String summary, Throwable error);
}
