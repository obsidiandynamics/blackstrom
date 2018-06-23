package com.obsidiandynamics.blackstrom.model;

final class UntypedMessage extends FluentMessage<UntypedMessage> {
  UntypedMessage(String xid, long timestamp) {
    super(xid, timestamp);
  }

  @Override 
  public MessageType getMessageType() {
    return null;
  }

  @Override 
  public String toString() {
    return UntypedMessage.class.getName() + " [" + baseToString() + "]";
  } 

  @Override
  public UntypedMessage shallowCopy() {
    return copyMutableFields(this, new UntypedMessage(getXid(), getTimestamp()));
  }
}