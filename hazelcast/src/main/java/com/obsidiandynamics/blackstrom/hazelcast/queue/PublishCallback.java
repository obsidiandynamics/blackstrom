package com.obsidiandynamics.blackstrom.hazelcast.queue;

public interface PublishCallback {
  void onComplete(long offset, Exception exception);
}
