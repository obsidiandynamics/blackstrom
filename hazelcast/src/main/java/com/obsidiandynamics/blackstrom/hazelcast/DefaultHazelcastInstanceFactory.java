package com.obsidiandynamics.blackstrom.hazelcast;

import com.hazelcast.config.*;
import com.hazelcast.core.*;

public final class DefaultHazelcastInstanceFactory implements HazelcastInstanceFactory {
  private static final DefaultHazelcastInstanceFactory instance = new DefaultHazelcastInstanceFactory();
  
  public static DefaultHazelcastInstanceFactory getInstance() {
    return instance;
  }
  
  private DefaultHazelcastInstanceFactory() {}
  
  @Override
  public HazelcastInstance create(Config config) {
    return Hazelcast.newHazelcastInstance(config);
  }
}
