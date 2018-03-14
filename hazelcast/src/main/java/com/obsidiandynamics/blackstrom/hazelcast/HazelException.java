package com.obsidiandynamics.blackstrom.hazelcast;

public abstract class HazelException extends Exception {
  private static final long serialVersionUID = 1L;

  protected HazelException(String m, Throwable cause) { super(m, cause); }
}
