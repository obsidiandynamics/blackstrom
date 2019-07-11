package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class VoteProcessorTest {
  @Test
  public void testNop() {
    final var proc = mock(VoteProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    final var context = mock(MessageContext.class);
    final var message = new Vote("xid", new Response("cohort-0", Intent.ACCEPT, null));
    proc.onVote(context, message);
    verifyZeroInteractions(context);
  }
  
  @Test
  public void testBeginAndConfirm() {
    final var proc = mock(VoteProcessor.BeginAndConfirm.class, Answers.CALLS_REAL_METHODS);
    final var context = mock(MessageContext.class);
    final var message = new Vote("xid", new Response("cohort-0", Intent.ACCEPT, null));
    proc.onVote(context, message);
    verify(context).beginAndConfirm(eq(message));
  }
}
