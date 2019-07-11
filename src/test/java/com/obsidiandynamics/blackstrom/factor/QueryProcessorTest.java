package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class QueryProcessorTest {
  @Test
  public void testNop() {
    final var proc = mock(QueryProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    final var context = mock(MessageContext.class);
    final var message = new Query("xid", "obj", 0);
    proc.onQuery(context, message);
    verifyZeroInteractions(context);
  }
  
  @Test
  public void testBeginAndConfirm() {
    final var proc = mock(QueryProcessor.BeginAndConfirm.class, Answers.CALLS_REAL_METHODS);
    final var context = mock(MessageContext.class);
    final var message = new Query("xid", "obj", 0);
    proc.onQuery(context, message);
    verify(context).beginAndConfirm(eq(message));
  }
}
