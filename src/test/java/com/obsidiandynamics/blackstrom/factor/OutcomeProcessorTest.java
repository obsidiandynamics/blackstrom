package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

public final class OutcomeProcessorTest {
  @Test
  public void testNop() {
    final OutcomeProcessor proc = mock(OutcomeProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onOutcome(null, null);
  }
}
