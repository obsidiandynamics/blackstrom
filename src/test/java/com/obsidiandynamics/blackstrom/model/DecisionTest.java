package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

public final class DecisionTest {
  @Test
  public void test() {
    final Response ra = new Response("a", Plea.ACCEPT, "a-meta");
    final Response rb = new Response("b", Plea.REJECT, "b-meta");
    final Decision d = new Decision(0, 1, "source", Verdict.COMMIT, 
                                    new Response[] {ra, rb});
    assertEquals(2, d.getResponses().length);
    assertSame(ra, d.getResponse("a"));
    assertSame(rb, d.getResponse("b"));
    assertNull(d.getResponse("c"));
  }
}
