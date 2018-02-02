package com.obsidiandynamics.blackstrom.codec;

import org.apache.commons.lang3.builder.*;

import com.esotericsoftware.kryo.*;

@DefaultSerializer(KryoCatSerializer.class)
public final class KryoCat extends KryoAnimal<KryoCat> {
  public String name;
  
  public KryoCat named(String name) {
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
    if (obj instanceof KryoCat) {
      final KryoCat that = (KryoCat) obj;
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
    return KryoCat.class.getSimpleName() + " [name=" + name + ", friend=" + friend + "]";
  }
}
