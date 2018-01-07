package com.obsidiandynamics.blackstrom.model;

import java.util.*;

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
  public String toString() {
    return "Outcome [verdict=" + verdict + ", responses=" + Arrays.toString(responses) + ", " + baseToString() + "]";
  }
}
