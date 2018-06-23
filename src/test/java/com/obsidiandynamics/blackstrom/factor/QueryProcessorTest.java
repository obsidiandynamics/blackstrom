package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

public final class QueryProcessorTest {
  @Test
  public void testNop() {
    final QueryProcessor proc = mock(QueryProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onQuery(null, null);
  }
}
