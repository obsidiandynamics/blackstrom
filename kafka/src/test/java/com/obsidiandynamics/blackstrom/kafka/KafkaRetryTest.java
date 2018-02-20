package com.obsidiandynamics.blackstrom.kafka;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.*;

import org.apache.kafka.common.*;
import org.junit.*;
import org.mockito.*;
import org.slf4j.*;

import com.obsidiandynamics.assertion.*;

public final class KafkaRetryTest {
  @Test
  public void testConformance() throws Exception {
    Assertions.assertUtilityClassWellDefined(KafkaRetry.class);
  }
  
  @Test
  public void testSuccess() {
    final Logger log = mock(Logger.class);
    KafkaRetry.run(1, log, () -> {});
    verifyNoMoreInteractions(log);
  }
  
  private static Runnable failFor(int attempts) {
    final AtomicInteger calls = new AtomicInteger();
    return () -> {
      final int call = calls.incrementAndGet();
      if (call <= attempts) throw new KafkaException("Failing on attempt " + call);
    };
  }
  
  @Test
  public void testSuccessAfterOneFailure() {
    final Logger log = mock(Logger.class);
    KafkaRetry.run(2, log, failFor(1));
    final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(log).warn(any(String.class), (Object[]) captor.capture());
    assertTrue(captor.getAllValues().size() > 1);
  }
  
  @Test(expected=KafkaException.class)
  public void testFailure() {
    final Logger log = mock(Logger.class);
    try {
      KafkaRetry.run(2, log, failFor(2));
    } finally {
      final ArgumentCaptor<Object> warnCaptor = ArgumentCaptor.forClass(Object.class);
      verify(log).warn(any(String.class), (Object[]) warnCaptor.capture());
      assertTrue(warnCaptor.getAllValues().size() > 1);
      final ArgumentCaptor<Object> errorCaptor = ArgumentCaptor.forClass(Object.class);
      verify(log).error(any(String.class), (Object[]) errorCaptor.capture());
      assertTrue(errorCaptor.getAllValues().size() > 1);
    }
  }
}
