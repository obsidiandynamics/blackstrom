package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

public final class NoticeProcessorTest {
  @Test
  public void testNop() {
    final NoticeProcessor proc = mock(NoticeProcessor.Nop.class, Answers.CALLS_REAL_METHODS);
    proc.onNotice(null, null);
  }
}
