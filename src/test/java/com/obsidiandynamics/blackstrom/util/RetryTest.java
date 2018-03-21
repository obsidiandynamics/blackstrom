package com.obsidiandynamics.blackstrom.util;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.*;

import org.junit.*;
import org.mockito.*;
import org.slf4j.*;

import com.obsidiandynamics.assertion.*;

public final class RetryTest {
  private static class TestRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    TestRuntimeException(String m) { super(m); }
  }
  
  private static Runnable failFor(int attempts) {
    final AtomicInteger calls = new AtomicInteger();
    return () -> {
      final int call = calls.incrementAndGet();
      if (call <= attempts) throw new TestRuntimeException("Failing on attempt " + call);
    };
  }
  
  @Test
  public void testSuccess() {
    final Logger log = mock(Logger.class);
    new Retry().withExceptionClass(TestRuntimeException.class).withAttempts(1).withLog(log).run(() -> {});
    verifyNoMoreInteractions(log);
  }
  
  @Test
  public void testSuccessAfterOneFailure() {
    final Logger log = mock(Logger.class);
    new Retry().withExceptionClass(TestRuntimeException.class).withBackoffMillis(0).withAttempts(2).withLog(log).run(failFor(1));
    final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(log).warn(any(), captor.capture());
    assertEquals(1, captor.getAllValues().size());
  }
  
  @Test(expected=TestRuntimeException.class)
  public void testFailure() {
    final Logger log = mock(Logger.class);
    try {
      new Retry().withExceptionClass(TestRuntimeException.class).withBackoffMillis(0).withAttempts(2).withLog(log).run(failFor(2));
    } finally {
      final ArgumentCaptor<Throwable> warnCaptor = ArgumentCaptor.forClass(Throwable.class);
      verify(log).warn(any(), warnCaptor.capture());
      assertEquals(1, warnCaptor.getAllValues().size());
      final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
      verify(log).error(any(), errorCaptor.capture());
      assertEquals(1, errorCaptor.getAllValues().size());
    }
  }
  
  @Test(expected=IllegalStateException.class) 
  public void testUncaughtRuntimeException() {
    final Logger log = mock(Logger.class);
    new Retry().withExceptionClass(TestRuntimeException.class).withLog(log).run(() -> {
      throw new IllegalStateException();
    });
    verifyNoMoreInteractions(log);
  }
  
  @Test
  public void testConfig() {
    final Retry r = new Retry()
        .withExceptionClass(TestRuntimeException.class)
        .withAttempts(10)
        .withBackoffMillis(20)
        .withLog(mock(Logger.class));
    Assertions.assertToStringOverride(r);
  }
  
  @Test
  public void testNoSleep() {
    Retry.sleepWithInterrupt(0);
    assertFalse(Thread.interrupted());
  }
  
  @Test
  public void testSleepUninterrupted() {
    Retry.sleepWithInterrupt(1);
    assertFalse(Thread.interrupted());
  }
  
  @Test
  public void testSleepInterrupted() {
    try {
      Thread.currentThread().interrupt();
      Retry.sleepWithInterrupt(1);
      assertTrue(Thread.interrupted());
    } finally {
      Thread.interrupted();
    }
  }
}
