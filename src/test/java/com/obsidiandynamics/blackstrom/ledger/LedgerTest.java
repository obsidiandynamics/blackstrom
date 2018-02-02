package com.obsidiandynamics.blackstrom.ledger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.*;

import org.junit.*;

public final class LedgerTest {
  @Test
  public void testExceptionLoggingAppendCallback() throws IOException {
    final PrintStream stream = mock(PrintStream.class);
    final AppendCallback c = Ledger.exceptionLoggingAppendCallback(stream);
    c.onAppend(null, null);
    verifyNoMoreInteractions(stream);
    
    c.onAppend(null, new Exception());
    verify(stream, atLeastOnce()).println(any(Object.class));
  }
}
