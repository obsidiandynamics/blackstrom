package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

public final class CommandResponse extends FluentMessage<CommandResponse> {
  private final Object result;
  
  public CommandResponse(String xid, Object result) {
    this(xid, NOW, result);
  }

  public CommandResponse(String xid, long timestamp, Object result) {
    super(xid, timestamp);
    this.result = result;
  }
  
  public <T> T getResult() {
    return Classes.cast(result);
  }
  
  @Override
  public MessageType getMessageType() {
    return MessageType.COMMAND_RESPONSE;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(baseHashCode())
        .append(result)
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
          .append(result, that.result)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return CommandResponse.class.getSimpleName() + " [" + baseToString() + ", result=" + result + "]";
  }
  
  @Override
  public CommandResponse shallowCopy() {
    return copyMutableFields(this, new CommandResponse(getXid(), getTimestamp(), result));
  }
}
