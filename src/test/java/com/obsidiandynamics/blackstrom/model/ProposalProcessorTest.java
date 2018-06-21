package com.obsidiandynamics.blackstrom.model;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.factor.*;

public final class ProposalProcessorTest {
  @Test
  public void testNop() {
    final ProposalProcessor proc = mock(ProposalProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onProposal(null, null);
  }
}
