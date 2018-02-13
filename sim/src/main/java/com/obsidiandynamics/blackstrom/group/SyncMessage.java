package com.obsidiandynamics.blackstrom.group;

import java.io.*;

import org.apache.commons.lang3.builder.*;

public abstract class SyncMessage implements Serializable {
  private static final long serialVersionUID = 1L;
  
  private final Serializable id;
  
  protected SyncMessage(Serializable id) {
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
    } else if (obj instanceof SyncMessage) {
      final SyncMessage that = (SyncMessage) obj;
      return new EqualsBuilder().append(id, that.id).isEquals();
    } else {
      return false;
    }
  }
}
