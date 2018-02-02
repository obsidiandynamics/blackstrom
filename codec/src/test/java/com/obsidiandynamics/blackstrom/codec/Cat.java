package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.esotericsoftware.kryo.*;
import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.*;
import com.obsidiandynamics.blackstrom.codec.kryo.*;

@DefaultSerializer(KryoCatSerializer.class)
@JsonSerialize(using=JacksonCatSerializer.class)
@JsonDeserialize(using=JacksonCatDeserializer.class)
public final class Cat extends Animal<Cat> {
  @JsonProperty
  public String name;
  
  public Cat named(String name) {
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
      final Cat that = (Cat) obj;
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
    return Cat.class.getSimpleName() + " [name=" + name + ", friend=" + friend + "]";
  }
}
