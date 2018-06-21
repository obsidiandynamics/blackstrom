package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

public final class CommandResponse extends FluentMessage<CommandResponse> {
  private final Object reply;
  
  public CommandResponse(String xid, Object reply) {
    this(xid, NOW, reply);
  }

  public CommandResponse(String xid, long timestamp, Object reply) {
    super(xid, timestamp);
    this.reply = reply;
  }
  
  public <T> T getObjective() {
    return Classes.cast(reply);
  }
  
  @Override
  public MessageType getMessageType() {
    return MessageType.COMMAND_RESPONSE;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(baseHashCode())
        .append(reply)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof CommandResponse) {
      final CommandResponse that = (CommandResponse) obj;
      return new EqualsBuilder()
          .appendSuper(baseEquals(that))
          .append(reply, that.reply)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return CommandResponse.class.getSimpleName() + " [" + baseToString() + ", reply=" + reply + "]";
  }
  
  @Override
  public CommandResponse shallowCopy() {
    return copyMutableFields(this, new CommandResponse(getBallotId(), getTimestamp(), reply));
  }
}
