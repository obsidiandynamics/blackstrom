package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class OutcomeTest {
  @Test
  public void test() {
    final Response ra = new Response("a", Pledge.ACCEPT, "a-meta");
    final Response rb = new Response("b", Pledge.REJECT, "b-meta");
    final Outcome outcome = new Outcome(1, Verdict.COMMIT, new Response[] {ra, rb});
    assertEquals(2, outcome.getResponses().length);
    assertSame(ra, outcome.getResponse("a"));
    assertSame(rb, outcome.getResponse("b"));
    assertNull(outcome.getResponse("c"));
    
    Assertions.assertToStringOverride(outcome);
  }
}
