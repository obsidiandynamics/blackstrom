package com.obsidiandynamics.blackstrom.hazelcast;

import com.hazelcast.config.*;
import com.hazelcast.core.*;

@FunctionalInterface
public interface HazelcastProvider {
  HazelcastInstance createInstance(Config config);
}
