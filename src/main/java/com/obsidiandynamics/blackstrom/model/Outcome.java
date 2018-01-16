package com.obsidiandynamics.blackstrom.model;

import java.util.*;

import org.apache.commons.lang3.builder.*;

public final class Outcome extends Message {
  private final Verdict verdict;
  private final AbortReason abortReason;
  private final Response[] responses;

  public Outcome(Object ballotId, Verdict verdict, AbortReason abortReason, Response[] responses) {
    this(ballotId, 0, verdict, abortReason, responses);
  }
  
  public Outcome(Object ballotId, long timestamp, Verdict verdict, AbortReason abortReason, Response[] responses) {
    super(ballotId, timestamp);
    assert verdict == Verdict.COMMIT ^ abortReason != null;
    assert responses != null;
    
    this.verdict = verdict;
    this.abortReason = abortReason;
    this.responses = responses;
  }
  
  public Verdict getVerdict() {
    return verdict;
  }
  
  public AbortReason getAbortReason() {
    return abortReason;
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
        .append(abortReason)
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
          .append(abortReason, that.abortReason)
          .append(responses, that.responses)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Outcome.class.getSimpleName() + " [verdict=" + verdict + ", abortReason=" + abortReason + 
        ", responses=" + Arrays.toString(responses) + ", " + baseToString() + "]";
  }
}
