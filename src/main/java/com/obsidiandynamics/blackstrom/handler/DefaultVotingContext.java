package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.ledger.*;

public final class DefaultVotingContext implements VotingContext {
  private final Ledger ledger;
  
  public DefaultVotingContext(Ledger ledger) {
    this.ledger = ledger;
  }

  @Override
  public Ledger getLedger() {
    return ledger;
  }
}
