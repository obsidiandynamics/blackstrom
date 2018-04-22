package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

public final class VoteTest {
  @Test
  public void testFields() {
    final Response ra = new Response("a", Intent.ACCEPT, "meta-a");
    final Vote v = new Vote("B1", ra);
    assertSame(ra, v.getResponse());
    
    Assertions.assertToStringOverride(v);
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(Vote.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
}
