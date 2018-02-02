package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.esotericsoftware.kryo.*;

@DefaultSerializer(KryoDogSerializer.class)
public final class KryoDog extends KryoAnimal<KryoDog> {
  public String name;
  
  public KryoDog named(String name) {
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
    if (obj instanceof KryoDog) {
      final KryoDog that = (KryoDog) obj;
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
    return KryoDog.class.getSimpleName() + " [name=" + name + ", friend=" + friend + "]";
  }
}
