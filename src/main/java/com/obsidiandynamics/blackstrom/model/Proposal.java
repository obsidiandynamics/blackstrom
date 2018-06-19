package com.obsidiandynamics.blackstrom.model;

import java.util.*;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

public final class Proposal extends FluentMessage<Proposal> {
  private final String[] cohorts;
  private final Object objective;
  
  /** The time to live, in milliseconds. */
  private final int ttl;

  public Proposal(String ballotId, String[] cohorts, Object objective, int ttl) {
    this(ballotId, 0, cohorts, objective, ttl);
  }

  public Proposal(String ballotId, long timestamp, String[] cohorts, Object objective, int ttl) {
    super(ballotId, timestamp);
    this.cohorts = cohorts;
    this.objective = objective;
    this.ttl = ttl;
  }
  
  public String[] getCohorts() {
    return cohorts;
  }

  public <T> T getObjective() {
    return Classes.cast(objective);
  }
  
  public int getTtl() {
    return ttl;
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
        .append(ttl)
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
          .append(ttl, that.ttl)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Proposal.class.getSimpleName() + " [" + baseToString() + ", cohorts=" + Arrays.toString(cohorts) + 
        ", objective=" + objective + ", ttl=" + ttl + "]";
  }
  
  @Override
  public Proposal clone() {
    return copyMutableFields(this, new Proposal(getBallotId(), getTimestamp(), cohorts, objective, ttl));
  }
}
