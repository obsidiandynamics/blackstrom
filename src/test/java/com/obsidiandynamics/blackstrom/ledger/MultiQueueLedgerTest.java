package com.obsidiandynamics.blackstrom.ledger;

public final class MultiQueueLedgerTest extends AbstractLedgerTest {
  @Override
  protected Ledger createLedger() {
    return new MultiQueueLedger();
  }
}
