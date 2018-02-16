package com.obsidiandynamics.blackstrom.util.throwing;

import org.junit.*;

public final class ThrowingRunnableTest {
  @Test
  public void testRun() throws Exception {
    final CheckedRunnable<Exception> r = CheckedRunnable::nop;
    r.run();
  }
}
