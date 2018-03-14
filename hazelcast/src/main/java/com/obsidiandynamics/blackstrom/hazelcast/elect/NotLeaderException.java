package com.obsidiandynamics.blackstrom.hazelcast.elect;

import com.obsidiandynamics.blackstrom.hazelcast.*;

public final class NotLeaderException extends HazelException {
  private static final long serialVersionUID = 1L;

  NotLeaderException(String m, Throwable cause) {
    super(m, cause);
  }
}
