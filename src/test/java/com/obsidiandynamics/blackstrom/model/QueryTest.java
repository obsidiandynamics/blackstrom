package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

public final class QueryTest {
  @Test
  public void testFields() {
    final Query p = new Query("B1", "objective", 1000);
    assertEquals("objective", p.getObjective());
    assertEquals(1000, p.getTtl());
    
    Assertions.assertToStringOverride(p);
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(Query.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
  
  @Test
  public void testShallowCopy() {
    final Query p = new Query("B1", "objective", 1000).withShardKey("shardKey");
    assertEquals(p, p.shallowCopy());
  }
}
