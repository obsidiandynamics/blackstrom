package com.obsidiandynamics.blackstrom.ledger;

import org.apache.commons.lang3.builder.*;

public final class ShardMessageId {
  private final int shard;
  
  private final long offset;

  public ShardMessageId(int shard, long offset) {
    this.shard = shard;
    this.offset = offset;
  }
  
  int getShard() {
    return shard;
  }

  long getOffset() {
    return offset;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(shard)
        .append(offset)
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof ShardMessageId) {
      final ShardMessageId that = (ShardMessageId) obj;
      return new EqualsBuilder()
          .append(shard, that.shard)
          .append(offset, that.offset)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return shard + "@" + offset;
  }
}
