package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.meteor.*;
import com.obsidiandynamics.zerolog.*;

public final class MeteorLedgerTest {
  @Test
  public void testAppendPackError() {
    final MessageCodec codec = new NullMessageCodec();
    final Publisher publisher = mock(Publisher.class);
    final Message m = new Proposal("100", new String[0], null, 0);
    final AppendCallback callback = mock(AppendCallback.class);
    MeteorLedger.appendWithCallback(codec, publisher, m, callback);
    
    verify(callback).onAppend(isNull(), isA(UnsupportedOperationException.class));
  }
  
  @Test
  public void testAppendCallbackError() {
    final MessageCodec codec = new IdentityMessageCodec();
    final Publisher publisher = mock(Publisher.class);
    final Exception cause = new Exception("test exception");
    doAnswer(invocation -> {
      final PublishCallback callback = invocation.getArgument(1);
      callback.onComplete(Record.UNASSIGNED_OFFSET, cause);
      return null;
    }).when(publisher).publishAsync(any(), any());
    
    final Message m = new Proposal("100", new String[0], null, 0);
    final AppendCallback callback = mock(AppendCallback.class);
    MeteorLedger.appendWithCallback(codec, publisher, m, callback);
    
    verify(callback).onAppend(isNull(), eq(cause));
  }

  @Test
  public void testReceivePackError() throws Exception {
    final Exception cause = new Exception("test exception");
    final MessageCodec codec = mock(MessageCodec.class);
    when(codec.encode(any())).thenReturn(new byte[0]);
    when(codec.decode(any())).thenThrow(cause);
    
    final MockLogTarget logTarget = new MockLogTarget();
    final Message m = new Proposal("100", new String[0], null, 0);
    final Record record = new Record(MessagePacker.pack(codec, m));
    final MessageHandler handler = mock(MessageHandler.class);
    final MessageContext context = mock(MessageContext.class);
    MeteorLedger.receive(codec, record, logTarget.logger(), handler, context);
    
    assertEquals(1, logTarget.entries().list().size());
    assertEquals(1, logTarget.entries().forLevel(LogLevel.ERROR).list().size());
    assertEquals(cause, logTarget.entries().forLevel(LogLevel.ERROR).list().get(0).getThrowable());
    verifyNoMoreInteractions(handler);
    verifyNoMoreInteractions(context);
  }
}
