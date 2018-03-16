package com.obsidiandynamics.blackstrom.hazelcast.queue;

public final class OffsetInitializationException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  OffsetInitializationException(String m) { super(m); }
}
