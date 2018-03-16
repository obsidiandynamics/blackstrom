package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.worker.*;

public interface Subscriber {
  RecordBatch poll(long timeoutMillis) throws InterruptedException;
  
  void confirm(long offset);
  
  void confirm();
  
  void seek(long offset);
  
  Joinable terminate();
  
  static DefaultSubscriber createDefault(HazelcastInstance instance, SubscriberConfig config) {
    return new DefaultSubscriber(instance, config);
  }
}
