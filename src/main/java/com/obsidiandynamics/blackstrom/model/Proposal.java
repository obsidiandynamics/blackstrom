package com.obsidiandynamics.blackstrom.model;

import java.util.*;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

public final class Proposal extends FluentMessage<Proposal> {
  private final String[] cohorts;
  private final Object objective;
  
  /** The time to live, in milliseconds. */
  private final int ttlMillis;

  public Proposal(String xid, String[] cohorts, Object objective, int ttlMillis) {
    this(xid, NOW, cohorts, objective, ttlMillis);
  }

  public Proposal(String xid, long timestamp, String[] cohorts, Object objective, int ttlMillis) {
    super(xid, timestamp);
    this.cohorts = cohorts;
    this.objective = objective;
    this.ttlMillis = ttlMillis;
  }
  
  public String[] getCohorts() {
    return cohorts;
  }

  public <T> T getObjective() {
    return Classes.cast(objective);
  }
  
  public int getTtl() {
    return ttlMillis;
  }
  
  @Override
  public MessageType getMessageType() {
    return MessageType.PROPOSAL;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(baseHashCode())
        .append(cohorts)
        .append(objective)
        .append(ttlMillis)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Proposal) {
      final Proposal that = (Proposal) obj;
      return new EqualsBuilder()
          .appendSuper(baseEquals(that))
          .append(cohorts, that.cohorts)
          .append(objective, that.objective)
          .append(ttlMillis, that.ttlMillis)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Proposal.class.getSimpleName() + " [" + baseToString() + ", cohorts=" + Arrays.toString(cohorts) + 
        ", objective=" + objective + ", ttl=" + ttlMillis + "]";
  }
  
  @Override
  public Proposal shallowCopy() {
    return copyMutableFields(this, new Proposal(getXid(), getTimestamp(), cohorts, objective, ttlMillis));
  }
}
