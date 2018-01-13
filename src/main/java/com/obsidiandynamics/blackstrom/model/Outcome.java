package com.obsidiandynamics.blackstrom.model;

import java.util.*;

import org.apache.commons.lang3.builder.*;

public final class Outcome extends Message {
  private final Verdict verdict;
  private final Response[] responses;

  public Outcome(Object ballotId, Verdict verdict, Response[] responses) {
    this(ballotId, 0, verdict, responses);
  }
  
  public Outcome(Object ballotId, long timestamp, Verdict verdict, Response[] responses) {
    super(ballotId, timestamp);
    this.verdict = verdict;
    this.responses = responses;
  }
  
  public Verdict getVerdict() {
    return verdict;
  }
  
  public Response[] getResponses() {
    return responses;
  }
  
  public Response getResponse(String cohort) {
    for (Response response : responses) {
      if (response.getCohort().equals(cohort)) {
        return response;
      }
    }
    return null;
  }

  @Override
  public MessageType getMessageType() {
    return MessageType.OUTCOME;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(verdict)
        .append(responses)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Outcome) {
      final Outcome that = (Outcome) obj;
      return new EqualsBuilder()
          .appendSuper(super.equals(obj))
          .append(verdict, that.verdict)
          .append(responses, that.responses)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Outcome.class.getSimpleName() + " [verdict=" + verdict + ", responses=" + Arrays.toString(responses) + 
        ", " + baseToString() + "]";
  }
}
