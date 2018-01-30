package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

public final class UnknownMessage extends FluentMessage<UnknownMessage> {
  public UnknownMessage(Object ballotId) {
    this(ballotId, 0);
  }
  
  public UnknownMessage(Object ballotId, long timestamp) {
    super(ballotId, timestamp);
  }

  @Override
  public MessageType getMessageType() {
    return MessageType.$UNKNOWN;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(super.hashCode())
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof UnknownMessage) {
      return new EqualsBuilder()
          .appendSuper(super.equals(obj))
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