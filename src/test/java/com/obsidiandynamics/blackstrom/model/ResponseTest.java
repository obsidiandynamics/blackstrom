package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class ResponseTest {
  @Test
  public void test() {
    final Response ra = new Response("a", Intent.ACCEPT, "a-meta"); 
    assertEquals("a", ra.getCohort());
    assertEquals(Intent.ACCEPT, ra.getIntent());
    assertEquals("a-meta", ra.getMetadata());
    Assertions.assertToStringOverride(ra);
  }
  
  @Test
  public void testEqualsHashCode() {
    final Response r1 = new Response("a", Intent.ACCEPT, "a-meta");
    final Response r2 = new Response("b", Intent.ACCEPT, "b-meta"); 
    final Response r3 = new Response("a", Intent.ACCEPT, "a-meta"); 
    final Response r4 = r1;

    assertNotEquals(r1, r2);
    assertEquals(r1, r3);
    assertEquals(r1, r4);
    assertNotEquals(r1, new Object());

    assertNotEquals(r1.hashCode(), r2.hashCode());
    assertEquals(r1.hashCode(), r3.hashCode());
  }
}
