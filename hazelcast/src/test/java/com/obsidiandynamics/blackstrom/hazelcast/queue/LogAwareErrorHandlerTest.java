package com.obsidiandynamics.blackstrom.hazelcast.queue;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;
import org.slf4j.*;

public final class LogAwareErrorHandlerTest {
  @Test
  public void test() {
    final Logger log = mock(Logger.class);
    final LogAwareErrorHandler handler = new LogAwareErrorHandler(() -> log);
    final String summary = "summary";
    final Throwable error = new RuntimeException("error");
    handler.onError(summary, error);
    verify(log).warn(eq(summary), eq(error));
  }
}
