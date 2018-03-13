package com.obsidiandynamics.blackstrom.hazelcast;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.test.*;

public final class MockHazelcastInstanceFactory implements HazelcastInstanceFactory {
  private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
  
  @Override
  public HazelcastInstance create(Config config) {
    return factory.newHazelcastInstance(config);
  }
}
