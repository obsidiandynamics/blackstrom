package com.obsidiandynamics.blackstrom.model;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.factor.*;

public final class OutcomeProcessorTest {
  @Test
  public void testNop() {
    final OutcomeProcessor proc = mock(OutcomeProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onOutcome(null, null);
  }
}
