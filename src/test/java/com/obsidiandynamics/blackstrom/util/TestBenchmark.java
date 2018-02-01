package com.obsidiandynamics.blackstrom.util;

import com.obsidiandynamics.indigo.util.*;

public final class TestBenchmark {
  private TestBenchmark() {}
  
  public static void setEnabled(Class<?> cls) {
    System.setProperty(cls.getSimpleName() + ".benchmark", String.valueOf(true));
  }
  
  public static boolean isEnabled(Class<?> cls) {
    return PropertyUtils.get(cls.getSimpleName() + ".benchmark", Boolean::valueOf, false);
  }
}
