package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class ProposalProcessorTest {
  @Test
  public void testNop() {
    final var proc = mock(ProposalProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    final var context = mock(MessageContext.class);
    final var message = new Proposal("xid", new String[] {}, "obj", 0);
    proc.onProposal(context, message);
    verifyZeroInteractions(context);
  }
  
  @Test
  public void testBeginAndConfirm() {
    final var proc = mock(ProposalProcessor.BeginAndConfirm.class, Answers.CALLS_REAL_METHODS);
    final var context = mock(MessageContext.class);
    final var message = new Proposal("xid", new String[] {}, "obj", 0);
    proc.onProposal(context, message);
    verify(context).beginAndConfirm(eq(message));
  }
}
