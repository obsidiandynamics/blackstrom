package com.obsidiandynamics.blackstrom.hazelcast.queue;

public enum InitialOffsetScheme {
  EARLIEST,
  LATEST,
  AUTO,
  NONE;
  
  InitialOffsetScheme resolveConcreteScheme(boolean useGroups) {
    if (this == AUTO) {
      return useGroups ? EARLIEST : LATEST;
    } else if (this == NONE) {
      return NONE;
    } else {
      return this;
    }
  }
}
