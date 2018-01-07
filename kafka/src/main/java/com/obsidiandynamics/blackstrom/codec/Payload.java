package com.obsidiandynamics.blackstrom.codec;

import com.obsidiandynamics.blackstrom.util.*;

public final class Payload {
  private final Object value;
  
  private Payload(Object value) {
    this.value = value;
  }
  
  public <T> T unpack() {
    return Cast.from(value);
  }
  
  public static Payload pack(Object value) {
    return value != null ? new Payload(value) : null;
  }
  
  public static <T> T unpack(Object obj) {
    if (obj == null) {
      return null;
    } else if (obj.getClass() == Payload.class) {
      return ((Payload) obj).unpack();
    } else {
      return Cast.from(obj);
    }
  }
  
  @Override
  public String toString() {
    return Payload.class.getSimpleName() + " [" + String.valueOf(value) + "]";
  }
}
