package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

public final class Query extends FluentMessage<Query> {
  private final Object objective;
  
  /** The time to live, in milliseconds. */
  private final int ttlMillis;

  public Query(String xid, Object objective, int ttlMillis) {
    this(xid, NOW, objective, ttlMillis);
  }

  public Query(String xid, long timestamp, Object objective, int ttlMillis) {
    super(xid, timestamp);
    this.objective = objective;
    this.ttlMillis = ttlMillis;
  }
  
  public <T> T getObjective() {
    return Classes.cast(objective);
  }
  
  public int getTtl() {
    return ttlMillis;
  }
  
  @Override
  public MessageType getMessageType() {
    return MessageType.QUERY;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(baseHashCode())
        .append(objective)
        .append(ttlMillis)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Query) {
      final Query that = (Query) obj;
      return new EqualsBuilder()
          .appendSuper(baseEquals(that))
          .append(objective, that.objective)
          .append(ttlMillis, that.ttlMillis)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Query.class.getSimpleName() + " [" + baseToString() + 
        ", objective=" + objective + ", ttl=" + ttlMillis + "]";
  }
  
  @Override
  public Query shallowCopy() {
    return copyMutableFields(this, new Query(getBallotId(), getTimestamp(), objective, ttlMillis));
  }
}
