package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class ResponseTest {
  @Test
  public void test() {
    final Response ra = new Response("a", Pledge.ACCEPT, "a-meta"); 
    assertEquals("a", ra.getCohort());
    assertEquals(Pledge.ACCEPT, ra.getPledge());
    assertEquals("a-meta", ra.getMetadata());
    Assertions.assertToStringOverride(ra);
  }
}
