package obsidiandynamics.blackstrom.model;

import java.util.*;

public final class Nomination extends Message {
  private final String[] cohorts;
  private final Object proposal;
  private final int ttl;

  public Nomination(Object messageId, Object ballotId, String source, String[] cohorts, Object proposal, int ttl) {
    super(messageId, ballotId, source);
    this.cohorts = cohorts;
    this.proposal = proposal;
    this.ttl = ttl;
  }
  
  public String[] getCohorts() {
    return cohorts;
  }

  public Object getProposal() {
    return proposal;
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
