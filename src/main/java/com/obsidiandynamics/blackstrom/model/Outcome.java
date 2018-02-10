package com.obsidiandynamics.blackstrom.model;

import java.util.*;

import org.apache.commons.lang3.builder.*;

public final class Outcome extends FluentMessage<Outcome> {
  private final Resolution resolution;
  private final AbortReason abortReason;
  private final Response[] responses;

  public Outcome(String ballotId, Resolution resolution, AbortReason abortReason, Response[] responses) {
    this(ballotId, 0, resolution, abortReason, responses);
  }
  
  public Outcome(String ballotId, long timestamp, Resolution resolution, AbortReason abortReason, Response[] responses) {
    super(ballotId, timestamp);
    this.resolution = resolution;
    this.abortReason = abortReason;
    this.responses = responses;
  }
  
  public Resolution getResolution() {
    return resolution;
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
        .append(resolution)
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
          .append(resolution, that.resolution)
          .append(abortReason, that.abortReason)
          .append(responses, that.responses)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Outcome.class.getSimpleName() + " [" + baseToString() + ", resolution=" + resolution + 
        ", abortReason=" + abortReason + ", responses=" + Arrays.toString(responses) + "]";
  }
}
