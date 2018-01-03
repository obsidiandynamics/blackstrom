package com.obsidiandynamics.blackstrom.ledger;

public final class DisruptorLedgerTest extends AbstractLedgerTest {
  @Override
  protected Ledger createLedgerImpl() {
    return new DisruptorLedger(1 << 20);
  }
}
