package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

public final class OutcomeTest {
  @Test
  public void test() {
    final Response ra = new Response("a", Plea.ACCEPT, "a-meta");
    final Response rb = new Response("b", Plea.REJECT, "b-meta");
    final Outcome outcome = new Outcome(0, 1, "source", Verdict.COMMIT, 
                                        new Response[] {ra, rb});
    assertEquals(2, outcome.getResponses().length);
    assertSame(ra, outcome.getResponse("a"));
    assertSame(rb, outcome.getResponse("b"));
    assertNull(outcome.getResponse("c"));
  }
}
