package com.obsidiandynamics.blackstrom.handler;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.retention.*;

public final class DefaultMessageContextTest {
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new DefaultMessageContext(null, null, NopRetention.getInstance()));
  }
  
  @Test
  public void testGet() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final Object handlerId = new Object();
    final Retention retention = NopRetention.getInstance();
    final MessageContext context = new DefaultMessageContext(ledger, handlerId, retention);
    assertEquals(ledger, context.getLedger());
    assertEquals(handlerId, context.getHandlerId());
    assertEquals(retention, context.getRetention());
  }
}
