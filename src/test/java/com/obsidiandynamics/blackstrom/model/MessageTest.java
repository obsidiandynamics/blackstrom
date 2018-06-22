package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.nanoclock.*;

import nl.jqno.equalsverifier.*;

public final class MessageTest {
  private static final class UntypedMessage extends FluentMessage<UntypedMessage> {
    UntypedMessage(String xid, long timestamp) {
      super(xid, timestamp);
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
    public UntypedMessage shallowCopy() {
      return copyMutableFields(this, new UntypedMessage(getXid(), getTimestamp()));
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

    assertEquals("B0", m.getXid());
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
      this(null, NOW);
    }
    
    private TrivialSubclass(String xid, long timestamp) {
      super(xid, timestamp);
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
    public TrivialSubclass shallowCopy() {
      return copyMutableFields(this, new TrivialSubclass(getXid(), getTimestamp()));
    }
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(TrivialSubclass.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
  
  @Test
  public void testBaseShallowCopy() {
    final Message m = new UntypedMessage("B0", 0)
        .withMessageId(new DefaultMessageId(0, 100))
        .withSource("test")
        .withShardKey("key")
        .withShard(99);

    
    final Message copy = m.shallowCopy();
    assertEquals(m.getXid(), copy.getXid());
    assertEquals(m.getMessageId(), copy.getMessageId());
    assertEquals(m.getSource(), copy.getSource());
    assertEquals(m.getShardKey(), copy.getShardKey());
    assertEquals(m.getShard(), copy.getShard());
    assertEquals(m.getTimestamp(), copy.getTimestamp());
  }
  
  @Test
  public void testFieldAssignmentToSame() {
    final Message m = new UntypedMessage("B0", 0)
        .withMessageId(new DefaultMessageId(0, 100))
        .withSource("test")
        .withShardKey("key")
        .withShard(99);
    
    m.setMessageId(m.getMessageId());
    m.setShard(m.getShard());
    m.setShardKey(m.getShardKey());
    m.setSource(m.getSource());
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testMessageIdReassignment() {
    final Message m = new UntypedMessage("B0", 0)
        .withMessageId(new DefaultMessageId(0, 100));
    m.setMessageId(new DefaultMessageId(0, 200));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testSourceReassignment() {
    final Message m = new UntypedMessage("B0", 0)
        .withSource("source");
    m.setSource("source2");
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testShardKeyReassignment() {
    final Message m = new UntypedMessage("B0", 0)
        .withShardKey("shardKey");
    m.setShardKey("shardKey2");
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testShardReassignment() {
    final Message m = new UntypedMessage("B0", 0)
        .withShard(100);
    m.setShard(200);
  }
}
