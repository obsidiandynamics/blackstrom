package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

public abstract class Message {
  private final Object ballotId;
  
  private final long timestamp;
  
  private Object messageId;
  
  private String source;
  
  protected Message(Object ballotId, long timestamp) {
    this.ballotId = ballotId;
    this.timestamp = timestamp != 0 ? timestamp : System.currentTimeMillis();
  }

  public final Object getBallotId() {
    return ballotId;
  }

  public final Object getMessageId() {
    return messageId;
  }
  
  public final Message withMessageId(Object messageId) {
    this.messageId = messageId;
    return this;
  }

  public final String getSource() {
    return source;
  }
  
  public final Message withSource(String source) {
    this.source = source;
    return this;
  }

  public final long getTimestamp() {
    return timestamp;
  }
  
  public abstract MessageType getMessageType();  
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(ballotId)
        .append(timestamp)
        .append(messageId)
        .append(source)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Message) {
      final Message other = (Message) obj;
      return new EqualsBuilder()
          .append(ballotId, other.ballotId)
          .append(timestamp, other.timestamp)
          .append(messageId, other.messageId)
          .append(source, other.source)
          .isEquals();
    } else {
      return false;
    }
  }
  
  protected final String baseToString() {
    return "ballotId=" + ballotId + ", messageId=" + messageId + ", source=" + source + ", timestamp="
        + timestamp;
  }
}
