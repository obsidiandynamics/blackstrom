package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

public final class MessageTest {
  private static final class TestMessage extends Message {
    TestMessage(Object ballotId, long timestamp) {
      super(ballotId, timestamp);
    }

    @Override public MessageType getMessageType() {
      return null;
    }

    @Override public String toString() {
      return TestMessage.class.getName() + " [" + baseToString() + "]";
    } 
  }

  @Test
  public void test() {
    final long time = System.currentTimeMillis();
    final Message m = new TestMessage(1, 0)
        .withMessageId(100)
        .withSource("test")
        .withShardKey("key")
        .withShardIndex(99);

    assertEquals(1, m.getBallotId());
    assertEquals(100, m.getMessageId());
    assertEquals("test", m.getSource());
    assertEquals("key", m.getShardKey());
    assertEquals((Integer) 99, m.getShardIndex());
    assertNull(m.getMessageType());
    assertTrue(m.getTimestamp() >= time);
  }

  @Test
  public void testEqualsHashcode() {
    final Message m1 = new TestMessage(1, 1000)
        .withMessageId(1).withSource("source").withShardKey("key").withShardIndex(10);
    final Message m2 = new TestMessage(0, 1000)
        .withMessageId(1).withSource("source").withShardKey("key").withShardIndex(10);
    final Message m3 = new TestMessage(1, 1000)
        .withMessageId(1).withSource("source").withShardKey("key").withShardIndex(10);
    final Message m4 = m1;

    assertNotEquals(m1, m2);
    assertEquals(m1, m3);
    assertEquals(m1, m4);
    assertNotEquals(m1, new Object());

    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertEquals(m1.hashCode(), m3.hashCode());
  }
}
