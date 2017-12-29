package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;

public final class DefaultInitContext implements InitContext {
  private final Ledger ledger;

  public DefaultInitContext(Ledger ledger) {
    this.ledger = ledger;
  }

  @Override
  public Ledger getLedger() {
    return ledger;
  }
}
