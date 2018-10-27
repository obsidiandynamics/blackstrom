package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.util.concurrent.*;

import org.hamcrest.core.*;
import org.junit.*;
import org.junit.rules.*;

public final class FutureAppendCallbackTest {
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();
  
  @Test
  public void testWithError() throws InterruptedException, ExecutionException {
    final FutureAppendCallback callback = new FutureAppendCallback();
    final Exception cause = new Exception("Simulated");
    callback.onAppend(null, cause);
    
    expectedException.expect(ExecutionException.class);
    expectedException.expectCause(Is.is(cause));
    callback.get();
  }
  
  @Test
  public void testWithMessageId() throws InterruptedException, ExecutionException {
    final FutureAppendCallback callback = new FutureAppendCallback();
    final DefaultMessageId messageId = new DefaultMessageId(0, 100);
    callback.onAppend(messageId, null);
    
    assertEquals(messageId, callback.get());
  }
  
  @Test
  public void testWithNullMessageId() throws InterruptedException, ExecutionException {
    final FutureAppendCallback callback = new FutureAppendCallback();
    callback.onAppend(null, null);
    
    assertNull(callback.get());
  }
}
