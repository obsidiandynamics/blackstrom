package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.annotation.JsonTypeInfo.*;
import com.obsidiandynamics.blackstrom.util.*;

@JsonTypeInfo(use=Id.NAME)
@JsonSubTypes({
  @JsonSubTypes.Type(name="Dog",value=JacksonDog.class),
  @JsonSubTypes.Type(name="Cat",value=JacksonCat.class)
})
public abstract class JacksonAnimal<A> {
  @JsonProperty
  public JacksonAnimal<?> friend;
  
  public A withFriend(JacksonAnimal<?> friend) {
    this.friend = friend;
    return Cast.from(this);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(friend)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JacksonAnimal) {
      final JacksonAnimal<?> that = (JacksonAnimal<?>) obj;
      return new EqualsBuilder()
          .append(friend, that.friend)
          .isEquals();
    } else {
      return false;
    }
  }
}
