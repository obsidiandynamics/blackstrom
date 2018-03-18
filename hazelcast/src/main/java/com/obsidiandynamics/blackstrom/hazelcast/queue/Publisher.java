package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.worker.*;

public interface Publisher extends Terminable {
  PublisherConfig getConfig();
  
  void publishAsync(Record record, PublishCallback callback);
  
  static DefaultPublisher createDefault(HazelcastInstance instance, PublisherConfig config) {
    return new DefaultPublisher(instance, config);
  }
}
