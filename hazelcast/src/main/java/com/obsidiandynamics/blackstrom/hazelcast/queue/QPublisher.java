package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.worker.*;

public interface QPublisher {
  void publishAsync(RawRecord record, PublishCallback callback);
  
  Joinable terminate();
  
  static DefaultQPublisher createDefault(HazelcastInstance instance, QPublisherConfig config) {
    return new DefaultQPublisher(instance, config);
  }
}
