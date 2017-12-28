package com.obsidiandynamics.blackstrom.ledger;

public final class SingleQueueLedgerTest extends AbstractLedgerTest {
  @Override
  protected Ledger createLedger() {
    return new SingleQueueLedger();
  }
}
