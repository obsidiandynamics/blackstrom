package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

@FunctionalInterface
public interface LeaseView {
  Map<String, Lease> asMap();
  
  default Lease getLease(String resource) {
    return asMap().getOrDefault(resource, Lease.vacant());
  }
  
  default boolean isCurrentTenant(String resource, UUID candidate) {
    return getLease(resource).isHeldByAndCurrent(candidate);
  }
}
