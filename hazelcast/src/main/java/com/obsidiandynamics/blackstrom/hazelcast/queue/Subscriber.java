package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.hazelcast.queue.Receiver.*;
import com.obsidiandynamics.blackstrom.worker.*;

public interface Subscriber extends Terminable {
  static int DEF_RECEIVER_POLL_TIMEOUT_MILLIS = 100;
  
  RecordBatch poll(long timeoutMillis) throws InterruptedException;
  
  SubscriberConfig getConfig();
  
  void confirm(long offset);
  
  void confirm();
  
  void seek(long offset);
  
  boolean isAssigned();
  
  void deactivate();
  
  void reactivate();
  
  default Receiver createReceiver(RecordHandler recordHandler) {
    return createReceiver(recordHandler, DEF_RECEIVER_POLL_TIMEOUT_MILLIS);
  }
  
  default Receiver createReceiver(RecordHandler recordHandler, int pollTimeoutMillis) {
    return new DefaultReceiver(this, recordHandler, pollTimeoutMillis);
  }
  
  static DefaultSubscriber createDefault(HazelcastInstance instance, SubscriberConfig config) {
    return new DefaultSubscriber(instance, config);
  }
}