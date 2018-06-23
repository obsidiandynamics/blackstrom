package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

public final class VoteProcessorTest {
  @Test
  public void testNop() {
    final VoteProcessor proc = mock(VoteProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onVote(null, null);
  }
}
