package com.obsidiandynamics.blackstrom.hazelcast.queue;

import org.junit.*;

public final class ErrorHandlerTest {
  @Test
  public void testNop() {
    // coverage only -- nothing to assert
    ErrorHandler.nop().onError(null, null);
  }
}
