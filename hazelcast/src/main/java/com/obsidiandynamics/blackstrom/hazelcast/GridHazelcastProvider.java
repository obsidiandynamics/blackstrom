package com.obsidiandynamics.blackstrom.hazelcast;

import com.hazelcast.config.*;
import com.hazelcast.core.*;

public final class GridHazelcastProvider implements HazelcastProvider {
  private static final GridHazelcastProvider instance = new GridHazelcastProvider();
  
  public static GridHazelcastProvider getInstance() {
    return instance;
  }
  
  private GridHazelcastProvider() {}
  
  @Override
  public HazelcastInstance createInstance(Config config) {
    return Hazelcast.newHazelcastInstance(config);
  }

  @Override
  public void shutdownAll() {
    Hazelcast.shutdownAll();
  }
}
