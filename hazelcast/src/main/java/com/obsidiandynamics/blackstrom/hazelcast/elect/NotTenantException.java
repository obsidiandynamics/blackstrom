package com.obsidiandynamics.blackstrom.hazelcast.elect;

import com.obsidiandynamics.blackstrom.hazelcast.*;

public final class NotTenantException extends HazelException {
  private static final long serialVersionUID = 1L;

  NotTenantException(String m, Throwable cause) {
    super(m, cause);
  }
}
