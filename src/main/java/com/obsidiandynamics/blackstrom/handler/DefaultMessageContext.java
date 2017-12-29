package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;

public final class DefaultMessageContext implements MessageContext {
  private final Ledger ledger;
  
  public DefaultMessageContext(Ledger ledger) {
    this.ledger = ledger;
  }

  @Override
  public Ledger getLedger() {
    return ledger;
  }
}
