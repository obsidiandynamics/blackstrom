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
}
