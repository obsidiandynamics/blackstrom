package com.obsidiandynamics.blackstrom.model;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.factor.*;

public final class QueryProcessorTest {
  @Test
  public void testNop() {
    final QueryProcessor proc = mock(QueryProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onQuery(null, null);
  }
}
