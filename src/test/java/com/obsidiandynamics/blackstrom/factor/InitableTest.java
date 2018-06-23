package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

public final class InitableTest {
  @Test
  public void testNop() {
    final Initable initable = mock(Initable.Nop.class, Answers.CALLS_REAL_METHODS);
    initable.init(null);
  }
}
