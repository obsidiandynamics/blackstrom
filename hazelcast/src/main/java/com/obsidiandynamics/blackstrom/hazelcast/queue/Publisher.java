package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.worker.*;

public interface Publisher {
  void publishAsync(Record record, PublishCallback callback);
  
  Joinable terminate();
  
  static DefaultPublisher createDefault(HazelcastInstance instance, PublisherConfig config) {
    return new DefaultPublisher(instance, config);
  }
}
