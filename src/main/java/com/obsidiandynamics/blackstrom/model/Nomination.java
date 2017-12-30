package com.obsidiandynamics.blackstrom.model;

import java.util.*;

import com.obsidiandynamics.blackstrom.util.*;

public final class Nomination extends Message {
  private final String[] cohorts;
  private final Object proposal;
  private final int ttl;

  public Nomination(Object ballotId, String[] cohorts, Object proposal, int ttl) {
    super(ballotId);
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
  public String toString() {
    return "Nomination [cohorts=" + Arrays.toString(cohorts) + ", proposal=" + proposal + ", ttl=" + ttl + ", " + baseToString() + "]";
  }
}
