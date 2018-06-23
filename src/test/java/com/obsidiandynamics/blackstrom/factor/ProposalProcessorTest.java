package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

public final class ProposalProcessorTest {
  @Test
  public void testNop() {
    final ProposalProcessor proc = mock(ProposalProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onProposal(null, null);
  }
}
