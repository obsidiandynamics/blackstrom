package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.*;

@JsonSerialize(using=JacksonCatSerializer.class)
@JsonDeserialize(using=JacksonCatDeserializer.class)
public final class JacksonCat extends JacksonAnimal<JacksonCat> {
  @JsonProperty
  public String name;
  
  public JacksonCat named(String name) {
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
    if (obj instanceof JacksonCat) {
      final JacksonCat that = (JacksonCat) obj;
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
    return JacksonCat.class.getSimpleName() + " [name=" + name + ", friend=" + friend + "]";
  }
}
