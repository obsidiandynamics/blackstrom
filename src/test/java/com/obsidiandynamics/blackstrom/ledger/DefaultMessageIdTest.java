package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import org.junit.*;

import nl.jqno.equalsverifier.*;

public final class DefaultMessageIdTest {
  @Test
  public void testGetters() {
    final DefaultMessageId messageId = new DefaultMessageId(2, 400);
    assertEquals(2, messageId.getShard());
    assertEquals(400, messageId.getOffset());
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(DefaultMessageId.class).verify();
  }
  
  @Test
  public void testToString() {
    assertEquals("2#400", new DefaultMessageId(2, 400).toString());
  }
}
