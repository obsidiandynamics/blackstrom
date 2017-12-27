package com.obsidiandynamics.blackstrom.util;

public final class Cast {
  private Cast() {}
  
  @SuppressWarnings("unchecked")
  public static <T> T from(Object obj) {
    return (T) obj;
  }
}
