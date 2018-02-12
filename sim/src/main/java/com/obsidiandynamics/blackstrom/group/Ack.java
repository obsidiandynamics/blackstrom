package com.obsidiandynamics.blackstrom.group;

import java.io.*;

public final class Ack extends SyncMessage {
  private static final long serialVersionUID = 1L;
  
  private Ack(Serializable id) {
    super(id);
  }

  public static Ack of(SyncMessage message) {
    return new Ack(message.getId());
  }
  
  @Override
  public String toString() {
    return Ack.class.getSimpleName() + "[" + baseToString() + "]";
  }
}
