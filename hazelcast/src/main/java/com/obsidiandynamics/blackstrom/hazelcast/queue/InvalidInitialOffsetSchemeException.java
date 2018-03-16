package com.obsidiandynamics.blackstrom.hazelcast.queue;

public final class InvalidInitialOffsetSchemeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  InvalidInitialOffsetSchemeException(String m) { super(m); }
}
