package com.obsidiandynamics.blackstrom.model;

import java.util.*;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.blackstrom.util.*;

public final class Proposal extends Message {
  private final String[] cohorts;
  private final Object objective;
  private final int ttl;

  public Proposal(Object ballotId, String[] cohorts, Object objective, int ttl) {
    this(ballotId, 0, cohorts, objective, ttl);
  }

  public Proposal(Object ballotId, long timestamp, String[] cohorts, Object objective, int ttl) {
    super(ballotId, timestamp);
    this.cohorts = cohorts;
    this.objective = objective;
    this.ttl = ttl;
  }
  
  public String[] getCohorts() {
    return cohorts;
  }

  public <T> T getObjective() {
    return Cast.from(objective);
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
        .appendSuper(super.hashCode())
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
          .appendSuper(super.equals(obj))
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
    return Proposal.class.getSimpleName() + " [cohorts=" + Arrays.toString(cohorts) + 
        ", objective=" + objective + ", ttl=" + ttl + ", " + baseToString() + "]";
  }
}
