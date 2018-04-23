package com.obsidiandynamics.blackstrom.ledger;

import org.junit.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class LedgerTest {
  @Test
  public void testDefaultMethods() {
    final Ledger ledger = new Ledger() {
      @Override
      public void attach(MessageHandler handler) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void append(Message message, AppendCallback callback) {}
    };
    ledger.init();
    ledger.confirm(null, null);
    ledger.append(null);
  }
}
