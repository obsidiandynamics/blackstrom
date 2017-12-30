package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

public final class MessageTest {
  @Test
  public void test() {
    final long time = System.currentTimeMillis();
    final Message m = new Message(1) {
      @Override public MessageType getMessageType() {
        return null;
      }
    };
    m.setMessageId(100);
    m.setSource("test");
    assertEquals(1, m.getBallotId());
    assertEquals(100, m.getMessageId());
    assertEquals("test", m.getSource());
    assertNull(m.getMessageType());
    assertTrue(m.getTimestamp() >= time);
  }
}
