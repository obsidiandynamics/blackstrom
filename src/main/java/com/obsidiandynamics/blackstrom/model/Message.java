package com.obsidiandynamics.blackstrom.model;

public abstract class Message {
  private final Object ballotId;
  
  private final long timestamp;
  
  private Object messageId;
  
  private String source;
  
  protected Message(Object ballotId) {
    this.ballotId = ballotId;
    timestamp = System.currentTimeMillis();
  }

  public final Object getBallotId() {
    return ballotId;
  }

  public final Object getMessageId() {
    return messageId;
  }
  
  public final void setMessageId(Object messageId) {
    this.messageId = messageId;
  }

  public final String getSource() {
    return source;
  }
  
  public final void setSource(String source) {
    this.source = source;
  }

  public final long getTimestamp() {
    return timestamp;
  }

  public abstract MessageType getMessageType();
  
  protected final String baseToString() {
    return "ballotId=" + ballotId + ", messageId=" + messageId + ", source=" + source + ", timestamp="
        + timestamp;
  }
}
