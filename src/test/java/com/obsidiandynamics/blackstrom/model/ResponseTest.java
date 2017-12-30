package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

public final class ResponseTest {
  @Test
  public void test() {
    final Response ra = new Response("a", Plea.ACCEPT, "a-meta"); 
    assertEquals("a", ra.getCohort());
    assertEquals(Plea.ACCEPT, ra.getPlea());
    assertEquals("a-meta", ra.getMetadata());
  }
}
