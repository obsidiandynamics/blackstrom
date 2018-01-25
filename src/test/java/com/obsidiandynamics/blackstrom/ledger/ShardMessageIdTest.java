package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class ShardMessageIdTest {
  @Test
  public void testGetters() {
    final ShardMessageId messageId = new ShardMessageId(2, 400);
    assertEquals(2, messageId.getShard());
    assertEquals(400, messageId.getOffset());
  }
  
  @Test
  public void testEqualsHashCode() {
    final ShardMessageId m1 = new ShardMessageId(2, 400);
    final ShardMessageId m2 = new ShardMessageId(3, 400);
    final ShardMessageId m3 = new ShardMessageId(2, 400);
    final ShardMessageId m4 = m1;
    
    assertNotEquals(m1, m2);
    assertEquals(m1, m3);
    assertEquals(m1, m4);
    assertNotEquals(m1, new Object());
    
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertEquals(m1.hashCode(), m3.hashCode());
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new ShardMessageId(2, 400));
  }
}
