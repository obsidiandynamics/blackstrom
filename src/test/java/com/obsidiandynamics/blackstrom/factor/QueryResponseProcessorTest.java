package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

public final class QueryResponseProcessorTest {
  @Test
  public void testNop() {
    final QueryResponseProcessor proc = mock(QueryResponseProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onQueryResponse(null, null);
  }
}
