package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

@FunctionalInterface
public interface LeadershipAssignmentHandler {
  void onAssign(String resource, UUID candidateId);
}
