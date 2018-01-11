package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.handler.*;

public class DefaultMessageContextTest {
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new DefaultMessageContext(null, null));
  }
  
  @Test
  public void testGet() {
    final Ledger ledger = new MultiNodeQueueLedger();
    final Object handlerId = new Object();
    final MessageContext context = new DefaultMessageContext(ledger, handlerId);
    assertEquals(ledger, context.getLedger());
    assertEquals(handlerId, context.getHandlerId());
  }
}
