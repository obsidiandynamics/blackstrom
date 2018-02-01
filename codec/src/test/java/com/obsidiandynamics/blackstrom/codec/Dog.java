package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.esotericsoftware.kryo.*;
import com.fasterxml.jackson.annotation.*;
import com.obsidiandynamics.blackstrom.codec.kryo.*;

@DefaultSerializer(KryoDogSerializer.class)
public final class Dog extends Animal<Dog> {
  @JsonProperty
  public String name;
  
  public Dog named(String name) {
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
