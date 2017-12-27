package obsidiandynamics.blackstrom.model;

public abstract class Message {
  private final Object messageId;
  
  private final Object ballotId;
  
  private final String source;
  
  private final long timestamp;
  
  protected Message(Object messageId, Object ballotId, String source) {
    this.messageId = messageId;
    this.ballotId = ballotId;
    this.source = source;
    timestamp = System.currentTimeMillis();
  }

  public final Object getMessageId() {
    return messageId;
  }

  public final Object getBallotId() {
    return ballotId;
  }

  public final String getSource() {
    return source;
  }

  public final long getTimestamp() {
    return timestamp;
  }

  public abstract MessageType getMessageType();
  
  protected final String baseToString() {
    return "messageId=" + messageId + ", ballotId=" + ballotId + ", source=" + source + ", timestamp="
        + timestamp;
  }
}
