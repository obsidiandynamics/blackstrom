package com.obsidiandynamics.blackstrom.util;

import com.obsidiandynamics.indigo.util.*;

public final class Testmark {
  private static final ThreadLocal<Boolean> enabled = ThreadLocal.withInitial(() -> false);
  
  private Testmark() {}
  
  public static void enable() {
    enabled.set(true);
  }
  
  public static boolean isEnabled() {
    return enabled.get();
  }
  
  public static void ifEnabled(ThrowingRunnable r) {
    if (isEnabled()) {
      System.out.println("Starting benchmark");
      try {
        r.run();
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }
}
