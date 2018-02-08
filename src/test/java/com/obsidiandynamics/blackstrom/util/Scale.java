package com.obsidiandynamics.blackstrom.util;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.resolver.*;

public final class Scale {
  public static final Supplier<Scale> UNITY = Singleton.of(Scale.by(1));
  
  private final int magnitude;
  
  private Scale(int magnitude) {
    this.magnitude = magnitude;
  }
  
  public int magnitude() {
    return magnitude;
  }
  
  public static Scale by(int magnitude) {
    return new Scale(magnitude);
  }
  
  public static Supplier<Scale> unity() {
    return UNITY;
  }
}
