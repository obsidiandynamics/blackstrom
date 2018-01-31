package com.obsidiandynamics.blackstrom.ledger;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class DefaultMessageId implements MessageId {
  private final int shard;
  
  private final long offset;

  public DefaultMessageId(long offset) {
    this(0, offset);
  }

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
    return new HashCodeBuilder()
        .append(shard)
        .append(offset)
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof DefaultMessageId) {
      final DefaultMessageId that = (DefaultMessageId) obj;
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
