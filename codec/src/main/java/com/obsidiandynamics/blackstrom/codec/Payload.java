package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

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
  public int hashCode() {
    return new HashCodeBuilder()
        .append(value)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Payload) {
      final Payload other = (Payload) obj;
      return new EqualsBuilder()
          .append(value,  other.value)
          .isEquals();
    } else {
      return false;
    }
  }
  
  @Override
  public String toString() {
    return Payload.class.getSimpleName() + " [" + String.valueOf(value) + "]";
  }
}
