package com.obsidiandynamics.blackstrom.ledger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

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
  
  @Test
  public void testDefaultMethods() {
    final Ledger ledger = new Ledger() {
      @Override
      public void attach(MessageHandler handler) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void append(Message message, AppendCallback callback) {}
    };
    ledger.init();
    ledger.confirm(null, null);
    ledger.append(null);
  }
}
