package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class VoteTest {
  @Test
  public void test() {
    final Response ra = new Response("a", Pledge.ACCEPT, "meta-a");
    final Vote v = new Vote(1, ra);
    assertSame(ra, v.getResponse());
    
    Assertions.assertToStringOverride(v);
  }
  
  @Test
  public void testEqualsHashCode() {
    final Vote v1 = new Vote(1, 1000, new Response("a", Pledge.ACCEPT, "meta-a"));
    final Vote v2 = new Vote(2, 1000, new Response("a", Pledge.ACCEPT, "meta-a"));
    final Vote v3 = new Vote(1, 1000, new Response("a", Pledge.ACCEPT, "meta-a"));
    final Vote v4 = v1;

    assertNotEquals(v1, v2);
    assertEquals(v1, v3);
    assertEquals(v1, v4);
    assertNotEquals(v1, new Object());

    assertNotEquals(v1.hashCode(), v2.hashCode());
    assertEquals(v1.hashCode(), v3.hashCode());
  }
}
