package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

public final class CommandTest {
  @Test
  public void testFields() {
    final Command p = new Command("B1", "objective", 1000);
    assertEquals("objective", p.getObjective());
    assertEquals(1000, p.getTtl());
    
    Assertions.assertToStringOverride(p);
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(Command.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
  
  @Test
  public void testShallowCopy() {
    final Command p = new Command("B1", "objective", 1000).withShardKey("shardKey");
    assertEquals(p, p.shallowCopy());
  }
}
