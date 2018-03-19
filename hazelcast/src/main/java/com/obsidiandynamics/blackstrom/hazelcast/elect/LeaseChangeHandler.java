package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

public interface LeaseChangeHandler {
  static LeaseChangeHandler nop = new LeaseChangeHandler() {
    @Override public void onExpire(String resource, UUID tenant) {}
    @Override public void onAssign(String resource, UUID tenant) {}
  };
  
  static LeaseChangeHandler nop() { return nop; }
  
  void onAssign(String resource, UUID tenant);

  void onExpire(String resource, UUID tenant);
}
