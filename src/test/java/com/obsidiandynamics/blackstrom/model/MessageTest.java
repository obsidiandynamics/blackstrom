package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

public final class MessageTest {
  private static final class UntypedMessage extends FluentMessage<UntypedMessage> {
    UntypedMessage(Object ballotId, long timestamp) {
      super(ballotId, timestamp);
    }

    @Override public MessageType getMessageType() {
      return null;
    }

    @Override public String toString() {
      return UntypedMessage.class.getName() + " [" + baseToString() + "]";
    } 
  }

  @Test
  public void testAttributes() {
    final long time = System.currentTimeMillis();
    final Message m = new UntypedMessage(1, 0)
        .withMessageId(100)
        .withSource("test")
        .withShardKey("key")
        .withShard(99);

    assertEquals(1, m.getBallotId());
    assertEquals(100, m.getMessageId());
    assertEquals("test", m.getSource());
    assertEquals("key", m.getShardKey());
    assertEquals(99, m.getShard());
    assertEquals((Integer) 99, m.getShardIfAssigned());
    assertNull(m.getMessageType());
    assertTrue(m.getTimestamp() >= time);
  }
  
  @Test
  public void testShardUnassignedAndCustomTime() {
    final long time = 1000;
    final Message m = new UntypedMessage(1, time)
        .withMessageId(100)
        .withSource("test")
        .withShardKey("key");
    
    assertEquals(-1, m.getShard());
    assertFalse(m.isShardAssigned());
    assertNull(m.getShardIfAssigned());
    assertEquals(1000, time);
  }

  @Test
  public void testEqualsHashcode() {
    final Message m1 = new UntypedMessage(1, 1000)
        .withMessageId(1).withSource("source").withShardKey("key").withShard(10);
    final Message m2 = new UntypedMessage(0, 1000)
        .withMessageId(1).withSource("source").withShardKey("key").withShard(10);
    final Message m3 = new UntypedMessage(1, 1000)
        .withMessageId(1).withSource("source").withShardKey("key").withShard(10);
    final Message m4 = m1;

    assertNotEquals(m1, m2);
    assertEquals(m1, m3);
    assertEquals(m1, m4);
    assertNotEquals(m1, new Object());

    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertEquals(m1.hashCode(), m3.hashCode());
  }
}
