package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class OutcomeTest {
  @Test
  public void testFields() {
    final Response ra = new Response("a", Intent.ACCEPT, "a-meta");
    final Response rb = new Response("b", Intent.REJECT, "b-meta");
    final Outcome outcome = new Outcome("B1", Resolution.ABORT, AbortReason.REJECT, new Response[] {ra, rb}, "metadata");
    assertEquals(Resolution.ABORT, outcome.getResolution());
    assertEquals(AbortReason.REJECT, outcome.getAbortReason());
    assertEquals(2, outcome.getResponses().length);
    assertSame(ra, outcome.getResponse("a"));
    assertSame(rb, outcome.getResponse("b"));
    assertNull(outcome.getResponse("c"));
    assertEquals("metadata", outcome.getMetadata());
    
    Assertions.assertToStringOverride(outcome);
  }
  
  @Test
  public void testEqualsHashCode() {
    final Response ra = new Response("a", Intent.ACCEPT, "a-meta");
    final Response rb = new Response("b", Intent.REJECT, "b-meta");
    final Outcome o1 = new Outcome("B1", 1000, Resolution.COMMIT, null, new Response[] {ra, rb}, null);
    final Outcome o2 = new Outcome("B1", 1000, Resolution.ABORT, AbortReason.IMPLICIT_TIMEOUT, new Response[] {ra, rb}, null);
    final Outcome o3 = new Outcome("B1", 1000, Resolution.COMMIT, null, new Response[] {ra, rb}, null);
    final Outcome o4 = o1;

    assertNotEquals(o1, o2);
    assertEquals(o1, o3);
    assertEquals(o1, o4);
    assertNotEquals(o1, new Object());

    assertNotEquals(o1.hashCode(), o2.hashCode());
    assertEquals(o1.hashCode(), o3.hashCode());
  }
}
