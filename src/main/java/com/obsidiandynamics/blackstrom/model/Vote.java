package com.obsidiandynamics.blackstrom.model;

public final class Vote extends Message {
  private final Response response;
  
  public Vote(Object ballotId, Response response) {
    this(ballotId, 0, response);
  }

  public Vote(Object ballotId, long timestamp, Response response) {
    super(ballotId, timestamp);
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
