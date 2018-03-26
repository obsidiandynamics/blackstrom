package com.obsidiandynamics.blackstrom.util;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.mockito.*;
import org.slf4j.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class RetryTest {
  private static class TestRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    TestRuntimeException(String m) { super(m); }
  }
  
  private static CheckedRunnable<RuntimeException> failFor(int attempts) {
    final AtomicInteger calls = new AtomicInteger();
    return () -> {
      final int call = calls.incrementAndGet();
      if (call <= attempts) throw new TestRuntimeException("Failing on attempt " + call);
    };
  }
  
  @Test
  public void testSuccess() {
    final Logger log = mock(Logger.class);
    final int answer = new Retry().withExceptionClass(TestRuntimeException.class).withAttempts(1).withLog(log).run(() -> 42);
    assertEquals(42, answer);
    verifyNoMoreInteractions(log);
  }
  
  @Test
  public void testSuccessAfterOneFailure() {
    final Logger log = mock(Logger.class);
    new Retry().withExceptionClass(TestRuntimeException.class).withBackoffMillis(0).withAttempts(2).withLog(log).run(failFor(1));
    final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(log).warn(any(), captor.capture());
    assertEquals(1, captor.getAllValues().size());
    verifyNoMoreInteractions(log);
  }
  
  @Test
  public void testFailureAndInterrupt() {
    final Logger log = mock(Logger.class);
    try {
      Thread.currentThread().interrupt();
      new Retry().withExceptionClass(TestRuntimeException.class).withBackoffMillis(0).withAttempts(2).withLog(log).run(failFor(1));
      fail("Did not throw expected exception");
    } catch (TestRuntimeException e) {
      assertTrue(Thread.interrupted());
      final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
      verify(log).error(any(), captor.capture());
      assertEquals(1, captor.getAllValues().size());
      verifyNoMoreInteractions(log);
    } finally {
      Thread.interrupted();
    }
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
      verifyNoMoreInteractions(log);
    }
  }
  
  private static int throwCheckedException() throws IOException, TimeoutException {
    throw new IOException("test exception");
  }
  
  @Test(expected=IOException.class)
  public void testUncaughtCheckedException() throws Exception {
    final Logger log = mock(Logger.class);
    final CheckedSupplier<Integer, Exception> supplier = RetryTest::throwCheckedException;
    try {
      new Retry().withExceptionClass(TestRuntimeException.class).withBackoffMillis(0).withAttempts(1).withLog(log).run(supplier);
    } finally {
      verifyNoMoreInteractions(log);
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
  public void testNoSleepUninterrupted() {
    assertTrue(Retry.sleepWithInterrupt(0));
    assertFalse(Thread.interrupted());
  }
  
  @Test
  public void testNoSleepInterrupted() {
    try {
      Thread.currentThread().interrupt();
      assertFalse(Retry.sleepWithInterrupt(0));
      assertTrue(Thread.interrupted());
    } finally {
      Thread.interrupted();
    }
  }
  
  @Test
  public void testSleepUninterrupted() {
    assertTrue(Retry.sleepWithInterrupt(1));
    assertFalse(Thread.interrupted());
  }
  
  @Test
  public void testSleepInterrupted() {
    try {
      Thread.currentThread().interrupt();
      assertFalse(Retry.sleepWithInterrupt(1));
      assertTrue(Thread.interrupted());
    } finally {
      Thread.interrupted();
    }
  }
}
