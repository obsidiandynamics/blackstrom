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
  
  @Test
  public void testEqualsHashCode() {
    final Response ra = new Response("a", Pledge.ACCEPT, "a-meta");
    final Response rb = new Response("b", Pledge.REJECT, "b-meta");
    final Outcome o1 = new Outcome(1, 1000, Verdict.COMMIT, new Response[] {ra, rb});
    final Outcome o2 = new Outcome(2, 1000, Verdict.COMMIT, new Response[] {ra, rb});
    final Outcome o3 = new Outcome(1, 1000, Verdict.COMMIT, new Response[] {ra, rb});
    final Outcome o4 = o1;

    assertNotEquals(o1, o2);
    assertEquals(o1, o3);
    assertEquals(o1, o4);
    assertNotEquals(o1, new Object());

    assertNotEquals(o1.hashCode(), o2.hashCode());
    assertEquals(o1.hashCode(), o3.hashCode());
  }
}
