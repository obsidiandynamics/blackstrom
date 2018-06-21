package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

public final class Notice extends FluentMessage<Notice> {
  private final Object event;
  
  public Notice(String xid, Object event) {
    this(xid, NOW, event);
  }

  public Notice(String xid, long timestamp, Object event) {
    super(xid, timestamp);
    this.event = event;
  }
  
  public <T> T getEvent() {
    return Classes.cast(event);
  }
  
  @Override
  public MessageType getMessageType() {
    return MessageType.NOTICE;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(baseHashCode())
        .append(event)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Notice) {
      final Notice that = (Notice) obj;
      return new EqualsBuilder()
          .appendSuper(baseEquals(that))
          .append(event, that.event)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Notice.class.getSimpleName() + " [" + baseToString() + ", event=" + event + "]";
  }
  
  @Override
  public Notice shallowCopy() {
    return copyMutableFields(this, new Notice(getBallotId(), getTimestamp(), event));
  }
}
