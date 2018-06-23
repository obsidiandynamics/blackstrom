package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.ledger.*;

public final class FluentMessageTest {
  @Test
  public void testFluent() {
    final Message m = new UntypedMessage("X0", 0)
        .withMessageId(new DefaultMessageId(0, 100))
        .withSource("test source")
        .withShardKey("key")
        .withShard(99);
    
    assertEquals(new DefaultMessageId(0, 100), m.getMessageId());
    assertEquals("test source", m.getSource());
    
    // tests FluentMessage.inResponseTo()
    final Message r = new UntypedMessage("X0", 0).inResponseTo(m);
    assertNull(r.getMessageId());
    assertNull(r.getSource());
    assertEquals("key", r.getShardKey());
    assertEquals(99, r.getShard());
  }
}
