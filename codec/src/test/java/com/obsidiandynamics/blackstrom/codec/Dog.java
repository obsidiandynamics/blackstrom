package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.fasterxml.jackson.annotation.*;

final class Dog extends Animal<Dog> {
  @JsonProperty
  String name;
  
  Dog named(String name) {
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
    if (obj instanceof Dog) {
      final Dog that = (Dog) obj;
      return new EqualsBuilder()
          .appendSuper(super.equals(obj))
          .append(name, that.name)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Dog.class.getSimpleName() + " [name=" + name + ", friend=" + friend + "]";
  }
}
