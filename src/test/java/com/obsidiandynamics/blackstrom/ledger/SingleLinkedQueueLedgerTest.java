package com.obsidiandynamics.blackstrom.ledger;

public final class SingleLinkedQueueLedgerTest extends AbstractLedgerTest {
  @Override
  protected Ledger createLedgerImpl() {
    return new SingleLinkedQueueLedger();
  }
}
