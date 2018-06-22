package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

public final class QueryResponseTest {
  @Test
  public void testFields() {
    final QueryResponse p = new QueryResponse("B1", "objective");
    assertEquals("objective", p.getResult());
    
    Assertions.assertToStringOverride(p);
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(QueryResponse.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
  
  @Test
  public void testShallowCopy() {
    final QueryResponse p = new QueryResponse("B1", "objective").withShardKey("shardKey");
    assertEquals(p, p.shallowCopy());
  }
}
