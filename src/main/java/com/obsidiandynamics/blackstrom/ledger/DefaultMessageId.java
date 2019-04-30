package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.model.*;

public final class DefaultMessageId implements MessageId {
  private final int shard;
  
  private final long offset;

  public DefaultMessageId(int shard, long offset) {
    this.shard = shard;
    this.offset = offset;
  }
  
  public int getShard() {
    return shard;
  }

  public long getOffset() {
    return offset;
  }
  
  @Override
  public int hashCode() {
    final var prime = 31;
    var result = 1;
    result = prime * result + (int) (offset ^ (offset >>> 32));
    result = prime * result + shard;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof DefaultMessageId) {
      final var that = (DefaultMessageId) obj;
      return shard == that.shard && offset == that.offset;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return shard + "@" + offset;
  }
}
