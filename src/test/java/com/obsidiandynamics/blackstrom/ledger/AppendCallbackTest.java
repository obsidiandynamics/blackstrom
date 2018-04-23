package com.obsidiandynamics.blackstrom.ledger;

import org.junit.*;

public final class AppendCallbackTest {
  @Test
  public void testNopCoverage() {
    AppendCallback.nop().onAppend(null, null);
  }
}
