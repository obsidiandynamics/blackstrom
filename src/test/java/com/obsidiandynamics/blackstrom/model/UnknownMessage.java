package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

public final class UnknownMessage extends FluentMessage<UnknownMessage> {
  public UnknownMessage(String ballotId) {
    this(ballotId, 0);
  }
  
  public UnknownMessage(String ballotId, long timestamp) {
    super(ballotId, timestamp);
  }

  @Override
  public MessageType getMessageType() {
    return MessageType.$UNKNOWN;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(baseHashCode())
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof UnknownMessage) {
      return new EqualsBuilder()
          .appendSuper(baseEquals((UnknownMessage) obj))
          .isEquals();
    } else {
      return false;
    }
  }
  
  @Override
  public String toString() {
    return UnknownMessage.class.getSimpleName() + " [" + baseToString() + "]";
  }
}