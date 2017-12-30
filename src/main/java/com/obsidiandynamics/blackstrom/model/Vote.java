package com.obsidiandynamics.blackstrom.model;

public final class Vote extends Message {
  private final Response response;

  public Vote(Object ballotId, Response response) {
    super(ballotId);
    this.response = response;
  }
  
  public Response getResponse() {
    return response;
  }

  @Override
  public MessageType getMessageType() {
    return MessageType.VOTE;
  }

  @Override
  public String toString() {
    return "Vote [response=" + response + ", " + baseToString() + "]";
  }
}
