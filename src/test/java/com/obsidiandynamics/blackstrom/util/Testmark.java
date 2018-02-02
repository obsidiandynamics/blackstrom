package com.obsidiandynamics.blackstrom.util;

import com.obsidiandynamics.indigo.util.*;

public final class Testmark {
  private Testmark() {}
  
  public static void setEnabled(Class<?> cls) {
    System.setProperty(cls.getSimpleName() + ".benchmark", String.valueOf(true));
  }
  
  public static boolean isEnabled(Class<?> cls) {
    return PropertyUtils.get(cls.getSimpleName() + ".benchmark", Boolean::valueOf, false);
  }
  
  public static class ConditionalRunner {
    private final Class<?> cls;
    
    ConditionalRunner(Class<?> cls) {
      this.cls = cls;
    }
    
    public void thenRun(ThrowingRunnable r) {
      if (isEnabled(cls)) {
        System.out.println("Starting benchmark " + cls.getSimpleName());
        try {
          r.run();
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  public static ConditionalRunner ifEnabled(Class<?> cls) {
    return new ConditionalRunner(cls);
  }
}
