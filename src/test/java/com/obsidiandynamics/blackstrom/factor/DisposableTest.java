package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.*;

public final class DisposableTest {
  @Test
  public void testNop() {
    final var disposable = mock(Disposable.Nop.class, Answers.CALLS_REAL_METHODS);
    disposable.dispose();
  }
  
  @Test
  public void testCloseable() {
    final var disposable = mock(Disposable.Nop.class, Answers.CALLS_REAL_METHODS);
    final var closeable = disposable.closeable();
    closeable.close();
    verify(disposable).dispose();
  }
}
