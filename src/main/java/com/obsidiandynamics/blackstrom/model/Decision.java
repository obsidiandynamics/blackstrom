package com.obsidiandynamics.blackstrom.model;

import java.util.*;

public final class Decision extends Message {
  private final Verdict verdict;
  private final Response[] responses;

  public Decision(Object messageId, Object ballotId, String source, Verdict verdict, Response[] responses) {
    super(messageId, ballotId, source);
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
    return MessageType.DECISION;
  }

  @Override
  public String toString() {
    return "Decision [verdict=" + verdict + ", responses=" + Arrays.toString(responses) + ", " + baseToString() + "]";
  }
}
