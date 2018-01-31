package com.obsidiandynamics.blackstrom.ledger;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class ShardMessageId implements MessageId {
  private final int shard;
  
  private final long offset;

  public ShardMessageId(long offset) {
    this(0, offset);
  }

  public ShardMessageId(int shard, long offset) {
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
