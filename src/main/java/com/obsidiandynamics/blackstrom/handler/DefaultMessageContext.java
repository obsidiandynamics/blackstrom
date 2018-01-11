package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;

public final class DefaultMessageContext implements MessageContext {
  private final Ledger ledger;
  
  private final Object handlerId;
  
  public DefaultMessageContext(Ledger ledger, Object handlerId) {
    this.ledger = ledger;
    this.handlerId = handlerId;
  }

  @Override
  public Ledger getLedger() {
    return ledger;
  }
  
  @Override
  public Object getHandlerId() {
    return handlerId;
  }

  @Override
  public String toString() {
    return DefaultMessageContext.class.getSimpleName() + " [ledger=" + ledger + ", handlerId=" + handlerId + "]";
  }
}
