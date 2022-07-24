package com.obsidiandynamics.blackstrom.ledger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.*;

import org.junit.*;

public final class AppendCallbackTest {
  @Test
  public void testNopCoverage() {
    AppendCallback.nop().onAppend(null, null);
  }
  
  @Test
  public void testErrorLoggingAppendCallback() {
    final PrintStream stream = mock(PrintStream.class);
    final AppendCallback c = AppendCallback.errorLoggingAppendCallback(stream);
    c.onAppend(null, null);
    verifyNoMoreInteractions(stream);
    
    c.onAppend(null, new Exception());
    verify(stream, atLeastOnce()).println(any(Object.class));
  }
}
