package obsidiandynamics.blackstrom.model;

public final class Vote extends Message {
  private final Response response;

  public Vote(Object messageId, Object ballotId, String source, Response response) {
    super(messageId, ballotId, source);
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
