package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

public final class NoticeTest {
  @Test
  public void testFields() {
    final Notice p = new Notice("B1", "objective");
    assertEquals("objective", p.getEvent());
    
    Assertions.assertToStringOverride(p);
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(Notice.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
  
  @Test
  public void testShallowCopy() {
    final Notice p = new Notice("B1", "objective").withShardKey("shardKey");
    assertEquals(p, p.shallowCopy());
  }
}
