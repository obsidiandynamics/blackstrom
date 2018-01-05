package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class KafkaMessageIdTest {
  @Test
  public void testGetters() {
    final KafkaMessageId messageId = new KafkaMessageId("test", 2, 400);
    assertEquals("test", messageId.getTopic());
    assertEquals(2, messageId.getPartition());
    assertEquals(400, messageId.getOffset());
  }
  
  @Test
  public void testEqualsHashCode() {
    final KafkaMessageId m1 = new KafkaMessageId("test", 2, 400);
    final KafkaMessageId m2 = new KafkaMessageId("test", 3, 400);
    final KafkaMessageId m3 = new KafkaMessageId("test", 2, 400);
    
    assertFalse("m1=" + m1 + ", m2=" + m2, m1.equals(m2));
    assertTrue("m1=" + m1 + ", m3=" + m3, m1.equals(m3));
    assertFalse(((Object) m1).equals("foo"));
    
    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertEquals(m1.hashCode(), m3.hashCode());
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new KafkaMessageId("test", 2, 400));
  }
}
