package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.retention.*;

public final class DefaultMessageContext implements MessageContext {
  private final Ledger ledger;
  
  private final Object handlerId;
  
  private final Retention retention;
  
  public DefaultMessageContext(Ledger ledger, Object handlerId, Retention retention) {
    this.ledger = ledger;
    this.handlerId = handlerId;
    this.retention = retention;
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
  public Retention getRetention() {
    return retention;
  }

  @Override
  public String toString() {
    return DefaultMessageContext.class.getSimpleName() + " [ledger=" + ledger + ", handlerId=" + handlerId + "]";
  }
}
