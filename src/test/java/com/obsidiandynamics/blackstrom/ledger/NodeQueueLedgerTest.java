package com.obsidiandynamics.blackstrom.ledger;

public final class NodeQueueLedgerTest extends AbstractLedgerTest {
  @Override
  protected Ledger createLedgerImpl() {
    return new NodeQueueLedger();
  }
}
