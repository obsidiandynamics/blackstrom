package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.nanoclock.*;

import nl.jqno.equalsverifier.*;

public final class MessageTest {
  private static final class UntypedMessage extends FluentMessage<UntypedMessage> {
    UntypedMessage(String ballotId, long timestamp) {
      super(ballotId, timestamp);
    }

    @Override 
    public MessageType getMessageType() {
      return null;
    }

    @Override 
    public String toString() {
      return UntypedMessage.class.getName() + " [" + baseToString() + "]";
    } 
    
    @Override
    public UntypedMessage clone() {
      return copyMutableFields(this, new UntypedMessage(getBallotId(), getTimestamp()));
    }
  }

  @Test
  public void testFields() {
    final long time = NanoClock.now();
    final Message m = new UntypedMessage("B0", 0)
        .withMessageId(new DefaultMessageId(0, 100))
        .withSource("test")
        .withShardKey("key")
        .withShard(99);

    assertEquals("B0", m.getBallotId());
    assertEquals(new DefaultMessageId(0, 100), m.getMessageId());
    assertEquals("test", m.getSource());
    assertEquals("key", m.getShardKey());
    assertEquals(99, m.getShard());
    assertEquals((Integer) 99, m.getShardIfAssigned());
    assertNull(m.getMessageType());
    assertTrue(m.getTimestamp() >= time);
  }
  
  @Test
  public void testShardAssigned() {
    final long time = 1000;
    final Message m = new UntypedMessage("B0", time).withShard(1);
    
    assertEquals(1, m.getShard());
    assertTrue(m.isShardAssigned());
  }
  
  @Test
  public void testShardUnassignedAndCustomTime() {
    final long time = 1000;
    final Message m = new UntypedMessage("B0", time)
        .withMessageId(new DefaultMessageId(0, 100))
        .withSource("test")
        .withShardKey("key");
    
    assertEquals(-1, m.getShard());
    assertFalse(m.isShardAssigned());
    assertNull(m.getShardIfAssigned());
    assertEquals(1000, time);
  }
  
  /**
   *  For testing of {@link Message#baseEquals(Message)} and {@link Message#baseHashCode()}.
   */
  private static final class TrivialSubclass extends Message {
    private TrivialSubclass() {
      this(null, 0);
    }
    
    private TrivialSubclass(String ballotId, long timestamp) {
      super(ballotId, timestamp);
    }
    
    @Override
    public int hashCode() {
      return baseHashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TrivialSubclass ? baseEquals((TrivialSubclass) obj) : false;
    }

    @Override
    public MessageType getMessageType() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public TrivialSubclass clone() {
      return copyMutableFields(this, new TrivialSubclass(getBallotId(), getTimestamp()));
    }
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(TrivialSubclass.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
}
