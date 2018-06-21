package com.obsidiandynamics.blackstrom.model;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.factor.*;

public final class VoteProcessorTest {
  @Test
  public void testNop() {
    final VoteProcessor proc = mock(VoteProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onVote(null, null);
  }
}
