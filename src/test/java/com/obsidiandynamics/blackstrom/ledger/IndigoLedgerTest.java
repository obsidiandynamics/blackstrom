package com.obsidiandynamics.blackstrom.ledger;

public final class IndigoLedgerTest extends AbstractLedgerTest {
  @Override
  protected Ledger createLedger() {
    return new IndigoLedger();
  }
}
