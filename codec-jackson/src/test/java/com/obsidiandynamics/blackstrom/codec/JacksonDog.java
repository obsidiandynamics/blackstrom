package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.*;

@JsonSerialize(using=JacksonDogSerializer.class)
@JsonDeserialize(using=JacksonDogDeserializer.class)
public final class JacksonDog extends JacksonAnimal<JacksonDog> {
  @JsonProperty
  public String name;
  
  public JacksonDog named(String name) {
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
    if (obj instanceof JacksonDog) {
      final JacksonDog that = (JacksonDog) obj;
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
    return JacksonDog.class.getSimpleName() + " [name=" + name + ", friend=" + friend + "]";
  }
}
