package com.obsidiandynamics.blackstrom.hazelcast.queue;

import com.hazelcast.core.*;
import com.obsidiandynamics.blackstrom.worker.*;

final class DefaultQPublisher implements QPublisher {
  private final HazelcastInstance instance;
  
  private final QPublisherConfig config;

  DefaultQPublisher(HazelcastInstance instance, QPublisherConfig config) {
    this.instance = instance;
    this.config = config;
  }

  @Override
  public void publishAsync(RawRecord record, PublishCallback callback) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Joinable terminate() {
    return Joinable.NOP;
  }
}
