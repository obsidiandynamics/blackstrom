package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.blackstrom.util.*;

public abstract class KryoAnimal<A> {
  public KryoAnimal<?> friend;
  
  public A withFriend(KryoAnimal<?> friend) {
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
    if (obj instanceof KryoAnimal) {
      final KryoAnimal<?> that = (KryoAnimal<?>) obj;
      return new EqualsBuilder()
          .append(friend, that.friend)
          .isEquals();
    } else {
      return false;
    }
  }
}
