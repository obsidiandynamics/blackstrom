package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

@FunctionalInterface
public interface LeaseView {
  Map<String, Lease> asMap();
  
  default UUID getTenant(String resource) {
    return asMap().getOrDefault(resource, Lease.VACANT).getTenant();
  }
  
  default boolean isCurrentTenant(String resource, UUID candidate) {
    return asMap().getOrDefault(resource, Lease.VACANT).isHeldByAndCurrent(candidate);
  }
}
