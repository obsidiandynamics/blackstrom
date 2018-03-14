package com.obsidiandynamics.blackstrom.hazelcast.elect;

import java.util.*;

final class LeaseViewImpl extends HashMap<String, Lease> implements LeaseView {
  private static final long serialVersionUID = 1L;
  
  LeaseViewImpl() {}
  
  LeaseViewImpl(LeaseViewImpl original) {
    super(original);
  }

  @Override
  public Map<String, Lease> asMap() {
    return this;
  }
}
