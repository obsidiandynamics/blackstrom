package com.obsidiandynamics.blackstrom.group;

import java.io.*;

import org.apache.commons.lang3.builder.*;

public abstract class SyncPacket implements Serializable {
  private static final long serialVersionUID = 1L;
  
  private final Serializable id;
  
  protected SyncPacket(Serializable id) {
    this.id = id;
  }
  
  public final Serializable getId() {
    return id;
  }
  
  public final String baseToString() {
    return "id=" + id;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(id).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof SyncPacket) {
      final SyncPacket that = (SyncPacket) obj;
      return new EqualsBuilder().append(id, that.id).isEquals();
    } else {
      return false;
    }
  }
}
