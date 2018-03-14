package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

@FunctionalInterface
public interface LeaseView {
  Map<String, Lease> asMap();
  
  default UUID getLeader(String resource) {
    return asMap().getOrDefault(resource, Lease.VACANT).getTenant();
  }
  
  default boolean isLeader(String resource, UUID candidateId) {
    return asMap().getOrDefault(resource, Lease.VACANT).isHeldByAndCurrent(candidateId);
  }
}
