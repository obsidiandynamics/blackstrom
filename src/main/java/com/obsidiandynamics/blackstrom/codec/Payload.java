package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import com.obsidiandynamics.func.*;

public final class Payload {
  private final Object value;
  
  private Payload(Object value) {
    this.value = mustExist(value, "Value cannot be null");
    mustBeFalse(value instanceof Payload, 
                withMessage(() -> "Cannot nest payload of type " + Payload.class.getSimpleName(), 
                            IllegalArgumentException::new));
  }
  
  public <T> T unpack() {
    return Classes.cast(value);
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
      return Classes.cast(obj);
    }
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Payload) {
      final var that = (Payload) obj;
      return Objects.equals(value, that.value);
    } else {
      return false;
    }
  }
  
  @Override
  public String toString() {
    return Payload.class.getSimpleName() + " [" + value + "]";
  }
}
