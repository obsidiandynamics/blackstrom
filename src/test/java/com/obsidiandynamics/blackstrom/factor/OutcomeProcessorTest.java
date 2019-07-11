package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class OutcomeProcessorTest {
  @Test
  public void testNop() {
    final var proc = mock(OutcomeProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    final var context = mock(MessageContext.class);
    final var message = new Outcome("xid", Resolution.ABORT, AbortReason.REJECT, new Response[] {}, null);
    proc.onOutcome(context, message);
    verifyZeroInteractions(context);
  }
  
  @Test
  public void testBeginAndConfirm() {
    final var proc = mock(OutcomeProcessor.BeginAndConfirm.class, Answers.CALLS_REAL_METHODS);
    final var context = mock(MessageContext.class);
    final var message = new Outcome("xid", Resolution.ABORT, AbortReason.REJECT, new Response[] {}, null);
    proc.onOutcome(context, message);
    verify(context).beginAndConfirm(eq(message));
  }
}
