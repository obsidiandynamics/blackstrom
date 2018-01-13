package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

public final class MessageTest {
  @Test
  public void test() {
    final long time = System.currentTimeMillis();
    final Message m = new Message(1, 0) {
      @Override public MessageType getMessageType() {
        return null;
      }
    };
    m.withMessageId(100);
    m.withSource("test");
    assertEquals(1, m.getBallotId());
    assertEquals(100, m.getMessageId());
    assertEquals("test", m.getSource());
    assertNull(m.getMessageType());
    assertTrue(m.getTimestamp() >= time);
  }
  
  @Test
  public void testEqualsHashcode() {
    final Message m1 = new Message(1, 0) { @Override public MessageType getMessageType() { return null; }};
    final Message m2 = new Message(0, 0) { @Override public MessageType getMessageType() { return null; }};
    final Message m3 = new Message(1, 0) { @Override public MessageType getMessageType() { return null; }};
    final Message m4 = m1;
    
    assertNotEquals(m1, m2);
    assertEquals(m1, m3);
    assertEquals(m1, m4);
    assertNotEquals(m1, new Object());

    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertEquals(m1.hashCode(), m3.hashCode());
  }
}
