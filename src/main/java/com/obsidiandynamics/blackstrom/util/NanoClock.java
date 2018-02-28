package com.obsidiandynamics.blackstrom.util;

public final class NanoClock {
  private static final long DIFF = System.currentTimeMillis() * 1_000_000 - System.nanoTime();
  
  private NanoClock() {}
  
  public static long now() {
    return System.nanoTime() + DIFF;
  }
}
