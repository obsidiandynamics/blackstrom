package com.obsidiandynamics.blackstrom.group;

import java.io.*;

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
}
