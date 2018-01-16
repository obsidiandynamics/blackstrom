package com.obsidiandynamics.blackstrom.model;

import java.util.*;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.blackstrom.util.*;

public final class Nomination extends Message {
  private final String[] cohorts;
  private final Object proposal;
  private final int ttl;

  public Nomination(Object ballotId, String[] cohorts, Object proposal, int ttl) {
    this(ballotId, 0, cohorts, proposal, ttl);
  }

  public Nomination(Object ballotId, long timestamp, String[] cohorts, Object proposal, int ttl) {
    super(ballotId, timestamp);
    assert cohorts != null;
    assert ttl >= 0;
    
    this.cohorts = cohorts;
    this.proposal = proposal;
    this.ttl = ttl;
  }
  
  public String[] getCohorts() {
    return cohorts;
  }

  public <T> T getProposal() {
    return Cast.from(proposal);
  }
  
  public int getTtl() {
    return ttl;
  }

  @Override
  public MessageType getMessageType() {
    return MessageType.NOMINATION;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(super.hashCode())
        .append(cohorts)
        .append(proposal)
        .append(ttl)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Nomination) {
      final Nomination that = (Nomination) obj;
      return new EqualsBuilder()
          .appendSuper(super.equals(obj))
          .append(cohorts, that.cohorts)
          .append(proposal, that.proposal)
          .append(ttl, that.ttl)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Nomination.class.getSimpleName() + " [cohorts=" + Arrays.toString(cohorts) + 
        ", proposal=" + proposal + ", ttl=" + ttl + ", " + baseToString() + "]";
  }
}
