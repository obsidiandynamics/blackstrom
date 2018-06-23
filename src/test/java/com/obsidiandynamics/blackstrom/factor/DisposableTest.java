package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.*;

public final class DisposableTest {
  @Test
  public void testNop() {
    final Disposable disposable = mock(Disposable.Nop.class, Answers.CALLS_REAL_METHODS);
    disposable.dispose();
  }
}
