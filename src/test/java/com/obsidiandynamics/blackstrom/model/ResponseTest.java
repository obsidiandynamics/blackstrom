package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

public final class ResponseTest {
  @Test
  public void testFields() {
    final Response ra = new Response("a", Intent.ACCEPT, "a-meta"); 
    assertEquals("a", ra.getCohort());
    assertEquals(Intent.ACCEPT, ra.getIntent());
    assertEquals("a-meta", ra.getMetadata());
    Assertions.assertToStringOverride(ra);
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(Response.class).verify();
  }
}
