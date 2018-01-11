package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.fasterxml.jackson.annotation.*;

final class Cat extends Animal<Cat> {
  @JsonProperty
  String name;
  
  Cat named(String name) {
    this.name = name;
    return this;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(super.hashCode())
        .append(name)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Cat) {
      final Cat other = (Cat) obj;
      return new EqualsBuilder()
          .appendSuper(super.equals(obj))
          .append(name, other.name)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Cat.class.getSimpleName() + " [name=" + name + ", friend=" + friend + "]";
  }
}
