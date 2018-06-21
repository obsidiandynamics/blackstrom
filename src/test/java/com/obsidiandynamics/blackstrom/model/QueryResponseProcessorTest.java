package com.obsidiandynamics.blackstrom.model;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.factor.*;

public final class QueryResponseProcessorTest {
  @Test
  public void testNop() {
    final QueryResponseProcessor proc = mock(QueryResponseProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onQueryResponse(null, null);
  }
}
