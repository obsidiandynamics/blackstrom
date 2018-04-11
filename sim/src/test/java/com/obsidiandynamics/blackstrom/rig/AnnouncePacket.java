package com.obsidiandynamics.blackstrom.rig;

import java.io.*;

import com.obsidiandynamics.jgroups.*;

public final class AnnouncePacket extends SyncPacket {
  private static final long serialVersionUID = 1L;
  
  private final String sandboxKey;

  public AnnouncePacket(Serializable id, String sandboxKey) {
    super(id);
    this.sandboxKey = sandboxKey;
  }
  
  public String getSandboxKey() {
    return sandboxKey;
  }

  @Override
  public String toString() {
    return AnnouncePacket.class.getSimpleName() + " [" + baseToString() + ", sandboxKey=" + sandboxKey + "]";
  }
}
