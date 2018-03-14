package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

public interface LeaseChangeHandler {
  void onAssign(String resource, UUID tenant);

  void onExpire(String resource, UUID tenant);
}
