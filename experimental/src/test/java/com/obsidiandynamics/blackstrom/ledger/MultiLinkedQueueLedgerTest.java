package com.obsidiandynamics.blackstrom.ledger;

public final class MultiLinkedQueueLedgerTest extends AbstractLedgerTest {
  @Override
  protected Ledger createLedgerImpl() {
    return new MultiLinkedQueueLedger();
  }
}
