package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.annotation.JsonTypeInfo.*;
import com.obsidiandynamics.blackstrom.util.*;

@JsonTypeInfo(use=Id.NAME)
@JsonSubTypes({
  @JsonSubTypes.Type(name="Dog",value=Dog.class),
  @JsonSubTypes.Type(name="Cat",value=Cat.class)
})
abstract class Animal<A> {
  @JsonProperty
  Animal<?> friend;
  
  A withFriend(Animal<?> friend) {
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
    if (obj instanceof Animal) {
      final Animal<?> that = (Animal<?>) obj;
      return new EqualsBuilder()
          .append(friend, that.friend)
          .isEquals();
    } else {
      return false;
    }
  }
}
