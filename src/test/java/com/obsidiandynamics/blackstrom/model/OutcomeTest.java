package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

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
    EqualsVerifier.forClass(Outcome.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
}
